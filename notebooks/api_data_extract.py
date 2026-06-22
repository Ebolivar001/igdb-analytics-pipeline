import requests
import pandas as pd
import json
import time
from pathlib import Path
from config import api_key
from IPython.display import display

BRONZE = Path("../data/bronze").resolve()
BRONZE.mkdir(parents=True, exist_ok=True)


valid_shards = ["steam", "console"]


# ### Steam Data Extraction


##Fetches a list of recent match IDs from the PUBG API (It is the only way to get a random, high-volume list of matches without knowing specific players beforehand)

def get_pubg_samples_steam(api_key, shard=valid_shards[0]):
    url = f"https://api.pubg.com/shards/{shard}/samples"
    headers = {
        "Authorization": f"Bearer {api_key}", 
        "Accept": "application/vnd.api+json"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None


match_samples = get_pubg_samples_steam(api_key)

print(match_samples)


matches_list = match_samples['data']['relationships']['matches']['data']
match_samples_df = pd.DataFrame(matches_list)
match_samples_df.rename(columns={'id': 'match_id'}, inplace=True)
print(f"Total matches retrieved: {len(match_samples_df)}")
display(match_samples_df.head())


match_id = match_samples_df['match_id'].tolist()


def get_match_details(api_key, match_id, shard=valid_shards[0]):
    """Fetch detailed information for a specific match."""
    url = f"https://api.pubg.com/shards/{shard}/matches/{match_id}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/vnd.api+json"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None


match_details = get_match_details(api_key,match_id[0])
print(match_details)


match_details = get_match_details(api_key,match_id[0])
matches_details_list = match_details['data']['relationships']['assets']['data']
print(matches_details_list)


def extract_telemetry_url(match_details):
    included_items = match_details.get("included", [])
    
    for item in included_items:
        is_telemetry = item.get("attributes", {}).get("name") == "telemetry"
        
        if is_telemetry:
            telemetry_url = item["attributes"]["URL"]
            print("Telemtry URL sucessfully extracted!")
            return telemetry_url
            
    print("Did not find telemetry URL in match details:(")
    return None


match_id_processed = match_id[0]
telemetry_url = extract_telemetry_url(match_details)
print(f"telemetry_url: {telemetry_url}")


def save_bronze_manifest(
    bronze_dir: Path,
    match_id: str,
    telemetry_url: str,
    match_details: dict,
) -> Path:
    """Save the current run's match metadata for the bronze loader."""
    if not telemetry_url:
        raise ValueError("No telemetry URL found — nothing to save.")

    bronze_dir.mkdir(parents=True, exist_ok=True)

    match_attrs = match_details.get("data", {}).get("attributes", {})

    manifest = {
        "match_id": match_id,
        "telemetry_url": telemetry_url,
        "map_name": match_attrs.get("mapName"),
        "game_mode": match_attrs.get("gameMode"),
    }

    manifest_path = bronze_dir / "manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)

    print(
        f"Manifest written to {manifest_path}. Run notebooks/bronze_load_to_postgres.py next."
    )

    return manifest_path


save_bronze_manifest(
    BRONZE,
    match_id_processed,
    telemetry_url,
    match_details,
)
