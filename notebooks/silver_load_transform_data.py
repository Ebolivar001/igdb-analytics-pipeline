import io
import json
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import psycopg
from azure.storage.blob import BlobServiceClient
from config import AZURE_CONNECTION_STRING, AZURE_CONTAINER
from config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

NOTEBOOKS_DIR = Path(__file__).resolve().parent
PROJECT_DIR = NOTEBOOKS_DIR.parent
SQL_DIR = NOTEBOOKS_DIR / "sql" / "silver"
DATASETS_CONFIG_PATH = SQL_DIR / "datasets.json"
MANIFEST_PATH = PROJECT_DIR / "data" / "bronze" / "manifest.json"

blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)


def current_ingest_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def load_datasets_config() -> dict:
    if not DATASETS_CONFIG_PATH.exists():
        raise FileNotFoundError(f"Datasets config not found: {DATASETS_CONFIG_PATH}")
    with open(DATASETS_CONFIG_PATH, encoding="utf-8") as handle:
        return json.load(handle)


def load_manifest(manifest_path: Path = MANIFEST_PATH) -> dict:
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Manifest not found at {manifest_path}. "
            "Run notebooks/api_data_extract.py first."
        )
    with open(manifest_path, encoding="utf-8") as handle:
        return json.load(handle)


def build_run_config() -> dict:
    config = load_datasets_config()
    manifest = load_manifest()

    config["match_id"] = manifest["match_id"]
    config["ingest_date"] = current_ingest_date()

    print(f"Loading match {config['match_id']} (ingest_date: {config['ingest_date']})")
    return config


def load_create_sql(dataset: str, config: dict) -> str:
    sql_file = config["datasets"][dataset]["sql_file"]
    sql_path = SQL_DIR / sql_file
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    return sql_path.read_text(encoding="utf-8")


def build_silver_blob_name(dataset: str, config: dict) -> str:
    return (
        f"silver/{config['silver_flow']}/{config['ingest_date']}/matches/"
        f"{config['match_id']}/{dataset}.parquet"
    )


def get_connection_config() -> dict:
    return {
        "host": POSTGRES_HOST,
        "port": POSTGRES_PORT,
        "dbname": POSTGRES_DB,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
    }


def verify_azure_source(dataset: str, config: dict) -> None:
    blob_name = build_silver_blob_name(dataset, config)
    blob_client = blob_service.get_blob_client(
        container=AZURE_CONTAINER,
        blob=blob_name,
    )
    if blob_client.exists():
        print(f"Found Azure source: {blob_name}")
    else:
        raise FileNotFoundError(f"Azure source not found: {blob_name}")


def create_table(cursor, dataset: str, config: dict) -> None:
    table_schema = config["table_schema"]
    full_table_name = f"{table_schema}.{dataset}"
    create_sql = load_create_sql(dataset, config).format(full_table_name=full_table_name)
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {table_schema}")
    cursor.execute(create_sql)
    print(f"Table ready: {full_table_name}")


def download_from_azure(dataset: str, config: dict) -> pd.DataFrame:
    blob_name = build_silver_blob_name(dataset, config)
    blob_client = blob_service.get_blob_client(
        container=AZURE_CONTAINER,
        blob=blob_name,
    )
    blob_data = blob_client.download_blob().readall()
    df = pd.read_parquet(io.BytesIO(blob_data))
    print(f"Downloaded {len(df)} rows from {blob_name}")
    return df


def normalize_zone(value):
    if hasattr(value, "tolist"):
        items = value.tolist()
        return None if not items else json.dumps(items)
    if pd.isna(value):
        return None
    return str(value)


TRANSFORMS = {
    "normalize_zone": lambda df: df.assign(zone=df["zone"].apply(normalize_zone)),
}


def apply_transforms(df: pd.DataFrame, dataset: str, config: dict) -> pd.DataFrame:
    prepared = df.copy()
    for transform_name in config["datasets"][dataset].get("transforms", []):
        if transform_name not in TRANSFORMS:
            raise ValueError(f"Unknown transform: {transform_name}")
        prepared = TRANSFORMS[transform_name](prepared)
    return prepared


def prepare_data(df: pd.DataFrame, dataset: str, config: dict) -> pd.DataFrame:
    prepared = df.copy()
    prepared["match_id"] = config["match_id"]
    prepared["ingest_date"] = config["ingest_date"]
    prepared = apply_transforms(prepared, dataset, config)
    columns = config["datasets"][dataset]["columns"]
    return prepared[columns]


def build_records(df: pd.DataFrame) -> list:
    records = []
    for row in df.itertuples(index=False, name=None):
        records.append(tuple(None if pd.isna(value) else value for value in row))
    return records


def insert_rows(cursor, records: list, dataset: str, config: dict) -> None:
    table_schema = config["table_schema"]
    columns = config["datasets"][dataset]["columns"]
    full_table_name = f"{table_schema}.{dataset}"
    column_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    insert_query = (
        f"INSERT INTO {full_table_name} ({column_list}) VALUES ({placeholders})"
    )
    cursor.executemany(insert_query, records)


def load_dataset_to_postgres(cursor, dataset: str, config: dict) -> int:
    verify_azure_source(dataset, config)
    create_table(cursor, dataset, config)

    df = prepare_data(download_from_azure(dataset, config), dataset, config)
    chunk_size = config["chunk_size"]

    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start : start + chunk_size]
        insert_rows(cursor, build_records(chunk), dataset, config)

    table_schema = config["table_schema"]
    print(
        f"Uploaded {len(df)} rows to {table_schema}.{dataset} "
        f"for match {config['match_id']}"
    )
    return len(df)


def load_data_to_postgres() -> None:
    config = build_run_config()
    datasets = list(config["datasets"].keys())

    with psycopg.connect(**get_connection_config()) as connection:
        with connection.cursor() as cursor:
            for dataset in datasets:
                load_dataset_to_postgres(cursor, dataset, config)
        connection.commit()


if __name__ == "__main__":
    load_data_to_postgres()
