import json
from datetime import datetime, timezone
from pathlib import Path

import requests
from azure.storage.blob import BlobServiceClient
from config import AZURE_CONNECTION_STRING, AZURE_CONTAINER
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

# --- Paths ---
project_dir = Path(__file__).resolve().parent.parent
bronze = project_dir / "data" / "bronze"
silver = project_dir / "data" / "silver"
telemetry_path = bronze / "telemetry.json"
manifest_path = bronze / "manifest.json"


def current_ingest_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("silver_match_flow").getOrCreate()


def load_manifest(manifest_path: Path) -> dict:
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Manifest not found at {manifest_path}. "
            "Run python3 notebooks/api_data_extract.py first."
        )

    with open(manifest_path, encoding="utf-8") as handle:
        manifest = json.load(handle)

    manifest["ingest_date"] = current_ingest_date()
    return manifest


def download_telemetry(telemetry_url: str) -> list:
    response = requests.get(telemetry_url, timeout=60)
    response.raise_for_status()
    return response.json()


def save_telemetry(events: list, telemetry_path: Path) -> None:
    telemetry_path.parent.mkdir(parents=True, exist_ok=True)
    with open(telemetry_path, "w", encoding="utf-8") as handle:
        json.dump(events, handle)
    print(f"Saved locally to {telemetry_path}")


def build_silver_blob_name(manifest: dict, filename: str) -> str:
    return (
        f"silver/silver_match_flow/{manifest['ingest_date']}/matches/"
        f"{manifest['match_id']}/{filename}"
    )


def get_silver_local_dir(manifest: dict) -> Path:
    return (
        silver
        / "silver_match_flow"
        / manifest["ingest_date"]
        / "matches"
        / manifest["match_id"]
    )


def upload_file(local_file: Path, blob_name: str) -> None:
    if not AZURE_CONNECTION_STRING:
        raise ValueError(
            "Set AZURE_CONNECTION_STRING in config.py, then re-run."
        )

    client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    blob = client.get_blob_client(container=AZURE_CONTAINER, blob=blob_name)

    with open(local_file, "rb") as handle:
        blob.upload_blob(handle.read(), overwrite=True)

    print(f"Uploaded to {AZURE_CONTAINER}/{blob_name}")


def save_and_upload_silver_df(
    df: DataFrame,
    manifest: dict,
    dataset_name: str,
) -> None:
    local_dir = get_silver_local_dir(manifest) / dataset_name
    local_dir.mkdir(parents=True, exist_ok=True)

    df.coalesce(1).write.mode("overwrite").parquet(str(local_dir))

    parquet_file = next(local_dir.glob("*.parquet"))
    blob_name = build_silver_blob_name(manifest, f"{dataset_name}.parquet")
    upload_file(parquet_file, blob_name)


def read_telemetry(spark: SparkSession, telemetry_path: Path) -> DataFrame:
    return spark.read.option("multiline", "true").json(str(telemetry_path))


def extract_player_positions(events_df: DataFrame) -> DataFrame:
    return (
        events_df
        .filter(col("_T") == "LogPlayerPosition")
        .select(
            col("elapsedTime").alias("elapsed_time"),
            col("character.name").alias("player_name"),
            col("character.location.x").alias("location_x"),
            col("character.location.y").alias("location_y"),
            col("character.location.z").alias("location_z"),
            col("character.zone").alias("zone"),
        )
    )


def extract_game_state(events_df: DataFrame) -> DataFrame:
    return (
        events_df
        .filter(col("_T") == "LogGameStatePeriodic")
        .select(
            col("gameState.elapsedTime").alias("elapsed_time"),
            col("gameState.safetyZonePosition.x").alias("safety_zone_x"),
            col("gameState.safetyZonePosition.y").alias("safety_zone_y"),
            col("gameState.safetyZoneRadius").alias("safety_zone_radius"),
            col("gameState.poisonGasWarningPosition.x").alias("warning_zone_x"),
            col("gameState.poisonGasWarningPosition.y").alias("warning_zone_y"),
            col("gameState.poisonGasWarningRadius").alias("warning_zone_radius"),
        )
    )

def extract_LogItemPickup(events_df: DataFrame) -> DataFrame:
    return (
        events_df
        .filter(col("_T") == "LogItemPickup")
        .select(
            col("_D").alias("ingested_at"),
            col("character.name").alias("character_name"),
            col("character.location.x").alias("location_x"),
            col("character.location.y").alias("location_y"),
            col("character.location.z").alias("location_z"),
            col("character.zone").alias("zone"),
            col("item.category").alias("category"),
            col("item.subCategory").alias("sub_category")
        )
    )

def extract_LogItemDrop(events_df: DataFrame) -> DataFrame:
    return (
        events_df
         .filter(col("_T") == "LogItemDrop")
        .select(
            col("_D").alias("ingested_at"),
            col("character.name").alias("character_name"),
            col("character.location.x").alias("location_x"),
            col("character.location.y").alias("location_y"),
            col("character.location.z").alias("location_z"),
            col("character.zone").alias("zone"),
            col("item.category").alias("category")
        )
    )  


def extract_LogVehicleRide(events_df: DataFrame) -> DataFrame:
    return (
        events_df
         .filter(col("_T") == "LogVehicleRide")
   .select(
            col("_D").alias("ingested_at"),
            col("character.name").alias("character_name"),
            col("character.accountId").alias("account_id"),
            col("character.teamId").alias("team_id"),
            col("character.health").alias("character_health"),
            col("character.location.x").alias("character_location_x"),
            col("character.location.y").alias("character_location_y"),
            col("character.location.z").alias("character_location_z"),
            col("character.zone").alias("zone"),
            col("seatIndex").alias("seat_index"),
            col("vehicle.vehicleType").alias("vehicle_type"),
            col("vehicle.vehicleId").alias("vehicle_id"),
            col("vehicle.seatIndex").alias("vehicle_seat_index"),
            col("vehicle.healthPercent").alias("vehicle_health_percent"),
            col("vehicle.feulPercent").alias("vehicle_fuel_percent"),
            col("vehicle.altitudeAbs").alias("vehicle_altitude_abs"),
            col("vehicle.altitudeRel").alias("vehicle_altitude_rel"),
            col("vehicle.velocity").alias("vehicle_velocity"),
            col("vehicle.isEngineOn").alias("is_engine_on"),
            col("vehicle.isInWaterVolume").alias("is_in_water_volume"),
            col("vehicle.isWheelsInAir").alias("is_wheels_in_air"),
            col("vehicle.location.x").alias("vehicle_location_x"),
            col("vehicle.location.y").alias("vehicle_location_y"),
            col("vehicle.location.z").alias("vehicle_location_z"),
        )
    )  

def extract_LogVehicleLeave(events_df: DataFrame) -> DataFrame:
    return (
        events_df
         .filter(col("_T") == "LogVehicleLeave")
    .select(
        col("_D").alias("ingested_at"),
        col("character.name").alias("character_name"),
        col("character.accountId").alias("account_id"),
        col("character.teamId").alias("team_id"),
        col("character.health").alias("character_health"),
        col("character.location.x").alias("character_location_x"),
        col("character.location.y").alias("character_location_y"),
        col("character.location.z").alias("character_location_z"),
        col("character.zone").alias("zone"),
        col("seatIndex").alias("seat_index"),
        col("rideDistance").alias("ride_distance"),
        col("maxSpeed").alias("max_speed"),
        col("vehicle.vehicleType").alias("vehicle_type"),
        col("vehicle.vehicleId").alias("vehicle_id"),
        col("vehicle.seatIndex").alias("vehicle_seat_index"),
        col("vehicle.healthPercent").alias("vehicle_health_percent"),
        col("vehicle.feulPercent").alias("vehicle_fuel_percent"),
        col("vehicle.altitudeAbs").alias("vehicle_altitude_abs"),
        col("vehicle.altitudeRel").alias("vehicle_altitude_rel"),
        col("vehicle.velocity").alias("vehicle_velocity"),
        col("vehicle.isEngineOn").alias("is_engine_on"),
        col("vehicle.isInWaterVolume").alias("is_in_water_volume"),
        col("vehicle.isWheelsInAir").alias("is_wheels_in_air"),
        col("vehicle.location.x").alias("vehicle_location_x"),
        col("vehicle.location.y").alias("vehicle_location_y"),
        col("vehicle.location.z").alias("vehicle_location_z"),
    )
    )  

def extract_LogPlayerAttack(events_df: DataFrame) -> DataFrame:
    return (
        events_df
         .filter(col("_T") == "LogPlayerAttack")
    .select(
        col("_D").alias("ingested_at"),
        col("attackId").alias("attack_id"),
        col("attackType").alias("attack_type"),
        col("fireWeaponStackCount").alias("fire_weapon_stack_count"),
        col("attacker.name").alias("attacker_name"),
        col("attacker.accountId").alias("attacker_account_id"),
        col("attacker.teamId").alias("attacker_team_id"),
        col("attacker.health").alias("attacker_health"),
        col("attacker.location.x").alias("attacker_location_x"),
        col("attacker.location.y").alias("attacker_location_y"),
        col("attacker.location.z").alias("attacker_location_z"),
        col("attacker.zone").alias("zone"),
        col("attacker.isInVehicle").alias("attacker_is_in_vehicle"),
        col("weapon.itemId").alias("weapon_id"),
        col("weapon.category").alias("weapon_category"),
        col("weapon.subCategory").alias("weapon_sub_category"),
        col("weapon.stackCount").alias("weapon_stack_count"),
        col("common.isGame").alias("is_game"),
    )
    )  

if __name__ == "__main__":
    manifest = load_manifest(manifest_path)

    if not telemetry_path.exists():
        print("Downloading telemetry...")
        save_telemetry(download_telemetry(manifest["telemetry_url"]), telemetry_path)
    else:
        print(f"Using cached telemetry: {telemetry_path}")

    spark = get_spark()
    events_df = read_telemetry(spark, telemetry_path)

    player_positions = extract_player_positions(events_df)
    game_state = extract_game_state(events_df)
    log_item_pickup = extract_LogItemPickup(events_df)
    log_item_drop = extract_LogItemDrop(events_df)
    log_vehicle_ride = extract_LogVehicleRide(events_df)
    log_vehicle_leave = extract_LogVehicleLeave(events_df)
    log_player_attack = extract_LogPlayerAttack(events_df)

    print(f"LogPlayerPosition rows: {player_positions.count()}")
    print(f"LogGameStatePeriodic rows: {game_state.count()}")
    print(f"LogItemPickup rows: {log_item_pickup.count()}")
    print(f"LogItemDrop rows: {log_item_drop.count()}")
    print(f"LogVehicleRide rows: {log_vehicle_ride.count()}")
    print(f"LogVehicleLeave rows: {log_vehicle_leave.count()}")
    print(f"LogPlayerAttack rows: {log_player_attack.count()}")
    print("Uploading silver datasets to Azure...")

    save_and_upload_silver_df(player_positions, manifest, "player_positions")
    save_and_upload_silver_df(game_state, manifest, "game_state")
    save_and_upload_silver_df(log_item_pickup, manifest, "log_item_pickup")
    save_and_upload_silver_df(log_item_drop, manifest, "log_item_drop")
    save_and_upload_silver_df(log_vehicle_ride, manifest, "log_vehicle_ride")
    save_and_upload_silver_df(log_vehicle_leave, manifest, "log_vehicle_leave")
    save_and_upload_silver_df(log_player_attack, manifest, "log_player_attack")
    spark.stop()
