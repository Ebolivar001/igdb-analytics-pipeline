import json
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import psycopg
from azure.storage.blob import BlobServiceClient
from psycopg.types.json import Jsonb

from config import AZURE_CONNECTION_STRING, AZURE_CONTAINER
from config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD


BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
BRONZE_DIR = PROJECT_DIR / "data" / "bronze"
MANIFEST_PATH = BRONZE_DIR / "manifest.json"

TABLE_SCHEMA = "stg_pubg"
TABLE_NAME = "raw_match_manifest"
FULL_TABLE_NAME = f"{TABLE_SCHEMA}.{TABLE_NAME}"
TABLE_COLUMNS = [
    "match_id",
    "telemetry_url",
    "ingest_date",
    "extracted_at",
    "match_created_at",
    "map_name",
    "game_mode",
    "raw_manifest",
]

CHUNK_SIZE = 10000


def current_ingest_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def current_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_local_manifest(manifest_path: Path = MANIFEST_PATH) -> dict:
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Manifest not found at {manifest_path}. Run notebooks/api_data_extract.py first."
        )

    with open(manifest_path, encoding="utf-8") as handle:
        manifest = json.load(handle)

    if not manifest.get("match_id"):
        raise ValueError("manifest.json has no match_id. Re-run notebooks/api_data_extract.py.")
    if not manifest.get("telemetry_url"):
        raise ValueError(
            "manifest.json has no telemetry_url. Re-run notebooks/api_data_extract.py."
        )

    return manifest


def build_manifest_blob_name(manifest: dict, ingest_date: str) -> str:
    return (
        f"bronze/{ingest_date}/matches/"
        f"{manifest['match_id']}/manifest.json"
    )


def get_blob_service() -> BlobServiceClient:
    if not AZURE_CONNECTION_STRING:
        raise ValueError("Set AZURE_CONNECTION_STRING in config.py, then re-run.")

    return BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)


def upload_manifest_to_azure(manifest_path: Path = MANIFEST_PATH) -> tuple:
    manifest = load_local_manifest(manifest_path)
    ingest_date = current_ingest_date()
    extracted_at = current_timestamp()
    blob_name = build_manifest_blob_name(manifest, ingest_date)
    blob_client = get_blob_service().get_blob_client(
        container=AZURE_CONTAINER,
        blob=blob_name,
    )
    manifest_data = json.dumps(manifest, indent=2)

    blob_client.upload_blob(manifest_data.encode("utf-8"), overwrite=True)

    print(f"Uploaded manifest to {AZURE_CONTAINER}/{blob_name}")
    return blob_name, ingest_date, extracted_at


def get_connection_config() -> dict:
    return {
        "host": POSTGRES_HOST,
        "port": POSTGRES_PORT,
        "dbname": POSTGRES_DB,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
    }


def create_table(cursor) -> None:
    # Create the staging schema and raw manifest table if they do not exist.
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {TABLE_SCHEMA}")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME} (
            match_id TEXT PRIMARY KEY,
            telemetry_url TEXT,
            ingest_date TEXT,
            extracted_at TIMESTAMPTZ,
            match_created_at TIMESTAMPTZ,
            map_name TEXT,
            game_mode TEXT,
            raw_manifest JSONB,
            ingested_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )


def download_manifest_from_azure(
    blob_name: str,
    ingest_date: str,
    extracted_at: str,
) -> pd.DataFrame:
    blob_client = get_blob_service().get_blob_client(
        container=AZURE_CONTAINER,
        blob=blob_name,
    )
    if not blob_client.exists():
        raise FileNotFoundError(f"Azure source not found: {blob_name}")

    print(f"Found Azure source: {blob_name}")
    blob_data = blob_client.download_blob().readall()
    manifest = json.loads(blob_data)
    manifest["raw_manifest"] = Jsonb(manifest.copy())
    manifest["ingest_date"] = ingest_date
    manifest["extracted_at"] = extracted_at
    return pd.DataFrame([manifest])


def prepare_data(match_manifests_df: pd.DataFrame) -> pd.DataFrame:
    # Clean date values before loading.
    match_manifests_df = match_manifests_df.copy()
    match_manifests_df["extracted_at"] = pd.to_datetime(
        match_manifests_df["extracted_at"],
        errors="coerce",
        utc=True,
    )
    match_manifests_df["match_created_at"] = pd.to_datetime(
        match_manifests_df["match_created_at"],
        errors="coerce",
        utc=True,
    )
    return match_manifests_df[TABLE_COLUMNS]


def build_records(match_manifests_df: pd.DataFrame) -> list:
    # Convert the df rows into tuples for inserting.
    records = []

    for row in match_manifests_df.itertuples(index=False, name=None):
        records.append(tuple(row))

    return records


def upsert_match_manifests(cursor, records: list) -> None:
    # Insert rows or update them when the match was already loaded.
    columns = ", ".join(TABLE_COLUMNS)
    placeholders = ", ".join(["%s"] * len(TABLE_COLUMNS))
    update_columns = [column for column in TABLE_COLUMNS if column != "match_id"]
    update_statement = ", ".join(
        f"{column} = EXCLUDED.{column}" for column in update_columns
    )

    upsert_query = f"""
        INSERT INTO {FULL_TABLE_NAME} ({columns})
        VALUES ({placeholders})
        ON CONFLICT (match_id)
        DO UPDATE SET
            {update_statement},
            ingested_at = NOW()
    """

    cursor.executemany(upsert_query, records)


def load_data_to_postgres() -> None:
    # Upload the latest local manifest first, then load that uploaded copy.
    blob_name, ingest_date, extracted_at = upload_manifest_to_azure()
    match_manifests_df = download_manifest_from_azure(
        blob_name,
        ingest_date,
        extracted_at,
    )
    match_manifests_df = prepare_data(match_manifests_df)

    with psycopg.connect(**get_connection_config()) as connection:
        with connection.cursor() as cursor:
            create_table(cursor)

            for start in range(0, len(match_manifests_df), CHUNK_SIZE):
                chunk = match_manifests_df.iloc[start:start + CHUNK_SIZE]
                records = build_records(chunk)
                upsert_match_manifests(cursor, records)

        connection.commit()

    print(f"Loaded {len(match_manifests_df)} manifest row(s) to {FULL_TABLE_NAME}")


if __name__ == "__main__":
    load_data_to_postgres()