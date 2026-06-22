CREATE TABLE IF NOT EXISTS {full_table_name} (
    match_id TEXT,
    ingest_date TEXT,
    elapsed_time DOUBLE PRECISION,
    player_name TEXT,
    location_x DOUBLE PRECISION,
    location_y DOUBLE PRECISION,
    location_z DOUBLE PRECISION,
    zone TEXT,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);
