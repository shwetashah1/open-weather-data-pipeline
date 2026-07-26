# Current Weather Data Ingestion Pipeline

Extracts postal codes from `mmai_db`, fetches current weather from OpenWeather,
cleans/engineers features, and loads into `uploads.current_weather` in your
personal PostgreSQL database.

## Setup

```bash
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in DB credentials + OPENWEATHER_API_KEY
```

## Run

```bash
python current_weather_pipeline.py --dry-run   # fetch + transform only, no DB writes
python current_weather_pipeline.py              # full run: creates table if needed, loads data
```

Safe to re-run — loads are idempotent (`ON CONFLICT DO NOTHING` on
`postal_code` + `measurement_timestamp`), so scheduling this daily will not
duplicate or overwrite existing rows.

## Files

| File | Purpose |
|---|---|
| `current_weather_pipeline.py` | ETL script: extract → transform → load |
| `sql/extract_postal_codes.sql` | Query run against `mmai_db` to select postal codes |
| `sql/create_tables.sql` | DDL for `uploads.current_weather` (also run automatically by the script) |
| `.env.example` | Template for required environment variables |
