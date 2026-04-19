import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode_outer, to_json
from src.api.igdb_client import IGDBClient
from src.config.settings import settings

def main():
    print(f"Starting ETL pipeline for {settings.SPARK_APP_NAME}...")

    # Ensure data directory exists
    os.makedirs("data", exist_ok=True)

    # 1. Extract Phase
    print("Extracting game data from IGDB API...")
    client = IGDBClient()
    if not client.token:
        print("Failed to authenticate. Check your .env file for valid IGDB_CLIENT_ID and IGDB_CLIENT_SECRET.")
        return

    # Using a query relevant to Game Optimization (requirements/platforms)
    query = "fields name, platforms.name, game_engines.name, summary, updated_at; limit 100;"
    games_data = client.get_games(query=query)

    if not games_data:
        print("No game data found or an error occurred during extraction.")
        return
        
    print(f"Successfully extracted {len(games_data)} game records.")

    # 2. Transform Phase
    print("Initializing Spark session for data transformation...")
    spark = SparkSession.builder \
        .appName(settings.SPARK_APP_NAME) \
        .master("local[*]") \
        .getOrCreate()

    # Convert list of dicts to Spark DataFrame
    df = spark.createDataFrame(games_data)
    
    # We want to flatten the platforms and game_engines if they exist, 
    # but for saving to parquet safely, we'll convert complex types to JSON strings 
    # or keep them as arrays of structs depending on requirements.
    # Here we cast them to string representations for easier downstream handling in simple DBs
    
    if "platforms" in df.columns:
        df = df.withColumn("platforms_json", to_json(col("platforms"))).drop("platforms")
        
    if "game_engines" in df.columns:
        df = df.withColumn("game_engines_json", to_json(col("game_engines"))).drop("game_engines")

    print("Data successfully transformed.")
    df.show(5, truncate=True)

    # 3. Load Phase
    output_path = "data/games_processed.parquet"
    print(f"Loading data into Parquet format at {output_path}...")
    
    # Write to parquet, overwriting if exists
    df.write.mode("overwrite").parquet(output_path)
    
    print("ETL pipeline completed successfully!")
    
    # Stop Spark
    spark.stop()

if __name__ == "__main__":
    main()
