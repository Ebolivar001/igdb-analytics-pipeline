import requests
import pandas as pd
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
    Parses a PUBG match JSON response into separate flattened DataFrames.
    Returns a dictionary of DataFrames: match, participants, rosters, assets.
    """
    if not match_json or 'data' not in match_json:
        return None

    match_data = match_json['data']
    df_match = pd.json_normalize(match_data)
    df_match.columns = [col.replace('attributes.', '').replace('relationships.', '') for col in df_match.columns]
    
    included = match_json.get('included', [])
    
    participants = [item for item in included if item['type'] == 'participant']
    rosters = [item for item in included if item['type'] == 'roster']
    assets = [item for item in included if item['type'] == 'asset']
    
    df_participants = pd.json_normalize(participants)
    if not df_participants.empty:
        df_participants.columns = [col.replace('attributes.', '').replace('stats.', '').replace('relationships.', '') for col in df_participants.columns]
        df_participants['match_id'] = match_data['id']
    
    df_rosters = pd.json_normalize(rosters)
    if not df_rosters.empty:
        df_rosters.columns = [col.replace('attributes.', '').replace('relationships.', '') for col in df_rosters.columns]
        df_rosters['match_id'] = match_data['id']
    
    df_assets = pd.json_normalize(assets)
    if not df_assets.empty:
        df_assets.columns = [col.replace('attributes.', '').replace('relationships.', '') for col in df_assets.columns]
        df_assets['match_id'] = match_data['id']
        
    return {
        'match': df_match,
        'participants': df_participants,
        'rosters': df_rosters,
        'assets': df_assets
    }

if __name__ == "__main__":
    # Example usage
    samples = get_pubg_samples(api_key)
    if samples:
        matches = samples['data']['relationships']['matches']['data']
        if matches:
            first_id = matches[0]['id']
            details = get_match_details(api_key, first_id)
            if details:
                dfs = parse_match_data(details)
                print(f"Successfully parsed match {first_id}")
                print(f"Participants: {len(dfs['participants'])}")
                print(f"Rosters: {len(dfs['rosters'])}")
