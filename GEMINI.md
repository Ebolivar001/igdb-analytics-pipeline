# GEMINI.md - PUBG Analytics Pipeline

## Project Overview
This project is an analytics platform for PUBG (PlayerUnknown's Battlegrounds) data. It handles telemetry extraction, player statistics, and match data using the official PUBG API, processing them via PySpark for downstream analysis.

## Style Guide and Agent Rules
- **Role:** Act as a Senior Data Engineer focused on the Modern Data Stack.
- **Language:** Write comments and documentation in clear, technical English.
- **Code Standards:** 
    - Always use Python Type Hints.
    - Strictly adhere to PEP 8.
    - For Spark, use the DataFrame API exclusively (avoid RDDs unless strictly necessary).
    - Implement robust error handling for API failures and Rate Limiting.

## Common Commands
### Installation and Setup
- Setup environment: `pip install -r requirements.txt`
- Export credentials: `export PUBG_API_KEY='your_key_here'`

### Pipeline Execution
- Raw Extraction (API -> JSON): `python src/extract/pubg_api_client.py`
- Spark Processing (Bronze -> Silver): `spark-submit src/transform/process_telemetry.py`
- Full execution: `make run` (if Makefile exists)

### Quality and Testing
- Run tests: `pytest`
- Linter: `flake8 src`
- Type checking: `mypy src`

## Data Architecture (Medallion)
- **Bronze:** Raw data in JSON/Parquet format extracted directly from the PUBG API.
- **Silver:** Cleaned data, explicitly defined schemas, null handling, and corrected data types.
- **Gold:** Aggregations by match, player, or season ready for Business Intelligence.

## PUBG API Specific Technical Notes
- **Rate Limits:** The API has strict limits (10 req/min for non-pro keys). Always implement `sleep` or `exponential backoff` mechanisms in extraction scripts.
- **Telemetry:** Telemetry files are massive and provided via external URLs. Downloading and processing them in parallel with Spark is a top priority.
- **Partitions:** Always partition data in S3/Local by `platform`, `date`, and `match_id`.
