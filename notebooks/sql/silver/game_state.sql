CREATE TABLE IF NOT EXISTS {full_table_name} (
    match_id TEXT,
    ingest_date TEXT,
    elapsed_time DOUBLE PRECISION,
    safety_zone_x DOUBLE PRECISION,
    safety_zone_y DOUBLE PRECISION,
    safety_zone_radius DOUBLE PRECISION,
    warning_zone_x DOUBLE PRECISION,
    warning_zone_y DOUBLE PRECISION,
    warning_zone_radius DOUBLE PRECISION,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);
