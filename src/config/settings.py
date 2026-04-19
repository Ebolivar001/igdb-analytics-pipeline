import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Settings:
    """Project-wide settings and environment variables."""
    IGDB_CLIENT_ID = os.getenv("IGDB_CLIENT_ID")
    IGDB_CLIENT_SECRET = os.getenv("IGDB_CLIENT_SECRET")
    
    # Example database configuration
    DB_URL = os.getenv("DB_URL", "sqlite:///igdb_analytics.db")

    # Spark settings
    SPARK_APP_NAME = "IGDB-Analytics-Pipeline"

settings = Settings()
