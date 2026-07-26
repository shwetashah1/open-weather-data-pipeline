# P2 - Weather Data Ingestion Pipeline: Execution Plan

## 1. Assignment Overview
Build a production-ready, scalable, and robust ETL Data Ingestion Pipeline in Python. The pipeline extracts location data (10-15 Toronto postal codes) from the course PostgreSQL database (`mmai_db`), ingests live weather data from an API (OpenWeather), cleans/transforms the data, and loads it idempotently into a personal PostgreSQL schema (`uploads.current_weather`).

## 2. Project Structure
```
db-assignment-3/
├── .env.example          # Template for required env vars (copy to .env, never commit .env)
├── .gitignore             # Ignores .env, venv/, __pycache__/
├── assignment.md
├── plan.md
├── requirements.txt
├── current_weather_pipeline.py    # Single-file ETL script (extract, transform, load)
├── sql/
│   ├── extract_postal_codes.sql   # Runs against the SOURCE db (mmai_db)
│   └── create_tables.sql          # Runs against the DESTINATION db (personal db)
└── img/
    └── Screenshot ....png
```
Kept as one Python script rather than a multi-module package — the pipeline is a linear extract → transform → load flow, and splitting it into a package would add indirection without adding capability at this scale.

## 3. Two Databases, Two Connections (critical correction)
The DataGrip screenshot shows `mmai_db` and the personal database (e.g. `preciado_db`) as **separate databases on the same Postgres server**. Postgres does not allow cross-database queries within a single connection, so the pipeline needs two distinct SQLAlchemy engines:

| Engine | Database | Used for |
|---|---|---|
| `source_engine` | `mmai_db` (read-only) | Running `sql/extract_postal_codes.sql` against `uploads.ca_postal_codes` |
| `dest_engine` | personal db (read/write) | Creating/loading `uploads.current_weather` |

Both sets of credentials live in `.env`, loaded via `python-dotenv`. This replaces the earlier assumption of a single connection/schema.

## 4. Environment Setup (Python 3.14.5)
`requirements.txt`:
```text
SQLAlchemy>=2.0.30
psycopg2-binary>=2.9.9
requests>=2.32.3
python-dotenv>=1.0.1
pandas>=2.2.2
tenacity>=8.3.0
```
**Setup commands:**
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env      # then fill in real credentials/API key
```

`.env.example` documents (not real secrets):
- `SOURCE_DB_HOST/PORT/NAME/USER/PASSWORD` — mmai_db connection
- `DEST_DB_HOST/PORT/NAME/USER/PASSWORD` — personal db connection
- `OPENWEATHER_API_KEY`
- Optional tuning: `OPENWEATHER_BASE_URL`, `REQUEST_TIMEOUT_SECONDS`, `RATE_LIMIT_CALLS_PER_MINUTE`, `SQL_FILE_PATH`, `LOG_LEVEL`

## 5. Database Schema & Table Setup (`sql/create_tables.sql`)
Runs against the **destination** (personal) database. The composite primary key makes inserts idempotent (safe to re-run without duplicating rows for the same postal code + measurement time).
```sql
CREATE SCHEMA IF NOT EXISTS uploads;

CREATE TABLE IF NOT EXISTS uploads.current_weather (
    province VARCHAR(50) NOT NULL,
    region VARCHAR(100) NOT NULL,
    postal_code VARCHAR(10) NOT NULL,
    latitude NUMERIC(9, 6) NOT NULL,
    longitude NUMERIC(9, 6) NOT NULL,
    temperature_celsius NUMERIC(5, 2),
    feels_like_celsius NUMERIC(5, 2),
    pressure_hpa INTEGER,
    humidity_pct INTEGER,
    weather_description VARCHAR(255),
    measurement_timestamp TIMESTAMPTZ NOT NULL,
    pipeline_run_timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (postal_code, measurement_timestamp)
);
```
The Python pipeline executes this file itself (`CREATE ... IF NOT EXISTS`) on every run so the destination is self-provisioning — no manual DataGrip step required, and re-running it is a no-op if the table already exists. (The assignment allows creating the schema manually in DataGrip; the script additionally guarantees it exists so the pipeline is not dependent on a manual setup step being remembered.)

## 6. Extraction Layer (`sql/extract_postal_codes.sql`)
Runs against the **source** database (`mmai_db`). Isolates the 10-15 representative postal codes.
```sql
SELECT province, region, postal_code, latitude, longitude
FROM uploads.ca_postal_codes
WHERE region = 'Toronto'
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL
ORDER BY postal_code
LIMIT 15;
```
To scale to all of Canada: remove the `WHERE`/`LIMIT` clauses (or parameterize `region`). The Python pipeline reads and executes this file as-is via SQLAlchemy and iterates over however many rows come back — no code changes needed to go from 15 rows to the full country.

## 7. Data Cleaning & Feature Engineering
The assignment (Instruction 4) requires both, as two distinct steps.

**Data cleaning** (applied per API response before loading):
- **Timestamp conversion:** API's `dt` field (Unix epoch, UTC) → timezone-aware `datetime` for `measurement_timestamp`; `pipeline_run_timestamp` recorded separately (`now(timezone.utc)`) so users can see *when the pipeline captured the row* vs. *when the measurement was taken*.
- **Unit/precision cleanup:** temperature and feels-like rounded to 2 decimals (API already returns °C via `units=metric`); pressure/humidity cast to `int`.
- **Missing/malformed field handling:** defensive `.get()` access on nested JSON (`main`, `weather[0]`); a row with missing required fields is logged and skipped rather than crashing the run.
- **Text normalization:** weather description trimmed and title-cased for consistent display/reporting.
- **Deduplication:** postal codes deduped before the API loop in case the source query returns overlapping rows.

**Feature engineering:**
- **`temp_category`:** `temperature_celsius` bucketed into `freezing` (<0°C) / `cold` (<10°C) / `mild` (<20°C) / `warm` (<28°C) / `hot` (≥28°C) — a genuine derived feature (not present in the raw API payload), useful downstream for route-risk-style filtering. This is what makes the pipeline satisfy the "feature engineering" half of Instruction 4, distinct from the cleaning steps above.

## 8. Production-Ready Pipeline Architecture
To meet the requirement of scaling from 15 postal codes to all of Canada:
- **Decoupled logic:** extraction SQL is data, not code — same script handles 15 or 15,000 rows.
- **Rate limiting:** `time.sleep()` between calls, derived from `RATE_LIMIT_CALLS_PER_MINUTE`, to stay under provider quotas.
- **Robust error handling (Tenacity):** exponential backoff retries on transient network errors.
- **Per-row fault isolation:** one postal code's failure (bad payload, API error after retries exhausted) is logged and skipped, not fatal to the whole run — important once this scales to thousands of postal codes.
- **Idempotency:** `INSERT ... ON CONFLICT (postal_code, measurement_timestamp) DO NOTHING` so scheduled daily runs never duplicate or overwrite existing rows.
- **Structured logging:** Python `logging` module (not `print`) with configurable level, so scheduled/cron runs produce inspectable logs.
- **Config via environment:** all secrets/tunables from `.env`, nothing hardcoded — same script runs locally or on a scheduler without code edits.

### Tenacity Implementation Example
```python
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(5))
def fetch_weather_for_postal_code(lat, lon, api_key):
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    response = requests.get(url, timeout=10)
    response.raise_for_status()  # 4xx/5xx triggers a retry
    return response.json()
```

## 9. Project Deliverables Checklist
- [ ] **`current_weather_pipeline.py`**: The main ETL Python script containing the extraction, transformation, API calling, and DB loading logic (must be `.py`, not `.ipynb`).
- [ ] **`sql/extract_postal_codes.sql`**: The specific SQL query used for extraction from `mmai_db`.
- [ ] **`sql/create_tables.sql`**: DDL for the destination table (also executed idempotently by the script).
- [ ] **SQL Tables**: The `uploads.current_weather` table created and successfully populated in the personal database.
- [ ] **Documentation Report (PDF)**: A maximum 1.5-page report summarizing the pipeline architecture, challenges overcome, and the daily scheduling strategy (written separately, not part of the code deliverables).
