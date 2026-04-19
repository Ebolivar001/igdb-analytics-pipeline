import requests
from src.config.settings import settings

class IGDBClient:
    """Client for interacting with the IGDB API."""

    def __init__(self, client_id=settings.IGDB_CLIENT_ID, client_secret=settings.IGDB_CLIENT_SECRET):
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = "https://api.igdb.com/v4"
        self.token = self._get_access_token()

    def _get_access_token(self):
        """Fetch an access token from Twitch (authentication provider for IGDB)."""
        auth_url = "https://id.twitch.tv/oauth2/token"
        params = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials"
        }
        try:
            response = requests.post(auth_url, params=params)
            response.raise_for_status()
            return response.json().get("access_token")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching access token: {e}")
            return None

    def get_games(self, query="fields name, platforms.name, game_engines.name, summary, updated_at; limit 50;"):
        """Fetch games from the IGDB games endpoint."""
        if not self.token:
            print("No access token available. Cannot fetch games.")
            return []
            
        headers = {
            "Client-ID": self.client_id,
            "Authorization": f"Bearer {self.token}"
        }
        try:
            response = requests.post(f"{self.base_url}/games", headers=headers, data=query)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching games: {e}")
            return []
