# IGDB Analytics Pipeline

A robust data engineering pipeline for extracting, transforming, and loading video game data from the Internet Game Database (IGDB) API.

## Project Architecture
- **Extract**: Python scripts using `requests` to pull data from IGDB API.
- **Transform**: PySpark for distributed data processing and cleaning.
- **Load**: Loading processed data into Parquet files or a database (e.g., PostgreSQL).

## Project Structure
```text
.
├── src/                # Modular Python source code
│   ├── api/            # API clients (IGDB)
│   ├── config/         # Configuration and environment settings
│   ├── extract_data/   # Extraction logic
│   └── load_data/      # Loading logic
├── notebooks/          # Exploratory Data Analysis and Spark testing
├── tests/              # Unit and integration tests
├── data/               # Local data storage (ignored by git)
├── requirements.txt    # Python dependencies
└── Makefile            # Common tasks (run, test, setup)
```

## Setup Instructions
1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd igdb-analytics-pipeline
   ```

2. **Create and activate a virtual environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Environment Variables**:
   Create a `.env` file in the root directory:
   ```env
   IGDB_CLIENT_ID=your_client_id
   IGDB_CLIENT_SECRET=your_client_secret
   ```

## Usage
- **Run the pipeline**: `python src/main.py`
- **Run tests**: `pytest`
- **Run notebooks**: `jupyter notebook`

## Future Improvements
- [ ] Implement CI/CD with GitHub Actions.
- [ ] Add Docker support for Spark cluster and Database.
- [ ] Implement data quality checks with Great Expectations.
- [ ] Schedule runs using Airflow or Prefect.
