import pandas as pd
import requests
import json
from notebooks.config import api_key

def get_pubg_samples(api_key, shard='steam'):
    """Fetch a list of sample matches from the PUBG API."""
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

def get_match_details(api_key, match_id, shard='steam'):
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

def parse_match_data(match_json):
    """
    Parses a PUBG match JSON response into separate flattened Pandas DataFrames.
    """
    if not match_json:
        return None

    # 1. Extract Core Match Data
    df_match = pd.json_normalize(match_json['data'])
    
    # 2. Extract Included Objects (Participants, Rosters, Assets)
    included_data = match_json.get('included', [])
    df_included = pd.json_normalize(included_data)
    df_included['match_id'] = match_json['data']['id']

    # Filter and Flatten Participants
    df_participants = df_included[df_included['type'] == 'participant'].copy()
    if not df_participants.empty:
        # Flatten and clean column names
        df_participants.columns = [c.replace('attributes.stats.', '') if c.startswith('attributes.stats.') else c for c in df_participants.columns]
        df_participants.columns = [c.replace('attributes.', '') if c.startswith('attributes.') else c for c in df_participants.columns]

    # Filter and Flatten Rosters
    df_rosters = df_included[df_included['type'] == 'roster'].copy()
    if not df_rosters.empty:
        df_rosters.columns = [c.replace('attributes.stats.', '') if c.startswith('attributes.stats.') else c for c in df_rosters.columns]
        df_rosters.columns = [c.replace('attributes.', '') if c.startswith('attributes.') else c for c in df_rosters.columns]
        # Relationship data handling
        if 'relationships.team.data' in df_rosters.columns:
            df_rosters = df_rosters.rename(columns={'relationships.team.data': 'team_data'})
        if 'relationships.participants.data' in df_rosters.columns:
            df_rosters = df_rosters.rename(columns={'relationships.participants.data': 'participants_data'})

    # Filter and Flatten Assets
    df_assets = df_included[df_included['type'] == 'asset'].copy()
    if not df_assets.empty:
        df_assets.columns = [c.replace('attributes.', '') if c.startswith('attributes.') else c for c in df_assets.columns]

    return {
        'match': df_match,
        'participants': df_participants,
        'rosters': df_rosters,
        'assets': df_assets
    }

def get_telemetry_data(telemetry_url):
    """Fetch telemetry data from the provided URL."""
    response = requests.get(telemetry_url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching telemetry: {response.status_code}")
        return None

def parse_telemetry_data(telemetry_json, match_id):
    """Parse telemetry events into a structured Pandas DataFrame."""
    if not telemetry_json:
        return None
    
    df_telemetry = pd.json_normalize(telemetry_json)
    df_telemetry['match_id'] = match_id
    
    return df_telemetry

if __name__ == "__main__":
    # Example usage
    samples = get_pubg_samples(api_key)
    if samples:
        matches = samples['data']['relationships']['matches']['data']
        if matches:
            # Process sample matches
            for match_ref in matches[:2]:
                match_id = match_ref['id']
                details = get_match_details(api_key, match_id)
                if details:
                    dfs = parse_match_data(details)
                    print(f"\n=== Processed Match {match_id} ===")
                    
                    # Get Telemetry URL from assets
                    assets_df = dfs['assets']
                    if not assets_df.empty and 'name' in assets_df.columns:
                        telemetry_row = assets_df[assets_df['name'] == 'telemetry']
                        if not telemetry_row.empty:
                            telemetry_url = telemetry_row['URL'].values[0]
                            print(f"Fetching telemetry from: {telemetry_url}")
                            telemetry_json = get_telemetry_data(telemetry_url)
                            
                            if telemetry_json:
                                df_telemetry = parse_telemetry_data(telemetry_json, match_id)
                                print(f"Telemetry events captured: {len(df_telemetry)}")
                                
                                # Show high-value event types (e.g., Kills)
                                if '_T' in df_telemetry.columns:
                                    print("Sample of Telemetry Event Types:")
                                    print(df_telemetry['_T'].value_counts().head(10))
                                    
                                    # Filter for specific interesting events
                                    if 'LogPlayerKill' in df_telemetry['_T'].values:
                                        print("\nSample Player Kill Events:")
                                        kill_events = df_telemetry[df_telemetry['_T'] == 'LogPlayerKill']
                                        cols_to_show = [c for c in ['victim.name', 'killer.name', 'damageTypeCategory'] if c in kill_events.columns]
                                        print(kill_events[cols_to_show].head(5))
