CREATE TABLE IF NOT EXISTS {full_table_name} (
    match_id TEXT,
    ingest_date TEXT,
    ingested_at TIMESTAMPTZ,
    character_name TEXT,
    location_x DOUBLE PRECISION,
    location_y DOUBLE PRECISION,
    location_z DOUBLE PRECISION,
    zone TEXT,
    category TEXT,
    sub_category TEXT
);
