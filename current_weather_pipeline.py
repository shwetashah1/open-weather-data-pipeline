"""Current Weather Data Ingestion Pipeline.

Extracts postal codes from the course database (mmai_db), fetches current
weather conditions for each from the OpenWeather API, cleans/transforms the
data, and loads it idempotently into uploads.current_weather in a personal
PostgreSQL database.

Designed to scale from a 10-15 postal code sample to all of Canada without
code changes: the extraction query is external SQL, API calls are rate
limited and retried individually, and a single bad row never aborts the run.

Usage:
    python current_weather_pipeline.py
    python current_weather_pipeline.py --sql-file sql/extract_postal_codes.sql --dry-run
"""

from __future__ import annotations

# CLI flags: --sql-file, --dry-run, --log-level
import argparse  
# structured, leveled logging instead of print() for scheduled/cron runs
import logging  
# reading configuration from environment variables
import os  
# process exit code (0/1) for schedulers/CI to detect failure
import sys  
# RateLimiter uses time.monotonic()/time.sleep() to throttle API calls
import time 
# immutable, typed Config container
from dataclasses import dataclass  
# converting API's Unix epoch to timezone-aware UTC timestamps
from datetime import datetime, timezone  
# filesystem paths for the .sql files, OS-independent
from pathlib import Path  
# loosely-typed JSON payloads from the weather API
from typing import Any  

# tabular handling of the extracted postal codes (dedup, NA-drop, read_sql)
import pandas as pd  
# HTTP client for the OpenWeather API
import requests  
# loads SOURCE_DB_*/DEST_DB_*/OPENWEATHER_* from .env into os.environ
from dotenv import load_dotenv  
# DB engines + safe execution of raw SQL files
from sqlalchemy import Engine, create_engine, text  
# builds connection URLs, escaping special characters in credentials
from sqlalchemy.engine import URL  
# INSERT ... ON CONFLICT DO NOTHING (idempotent load)
from sqlalchemy.dialects.postgresql import insert as pg_insert  
# catching DB-layer errors distinctly from API/logic errors
from sqlalchemy.exc import SQLAlchemyError  
# retry with exponential backoff for transient weather-API failures
from tenacity import (  
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger("current_weather_pipeline")

DEFAULT_SQL_FILE_PATH = "sql/extract_postal_codes.sql"
DEFAULT_CREATE_TABLE_SQL_PATH = "sql/create_tables.sql"
DEFAULT_OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
DEFAULT_REQUEST_TIMEOUT_SECONDS = 10
DEFAULT_RATE_LIMIT_CALLS_PER_MINUTE = 55

TARGET_TABLE = "uploads.current_weather"
REQUIRED_POSTAL_CODE_COLUMNS = ("province", "region", "postal_code", "latitude", "longitude")


class PipelineConfigError(RuntimeError):
    """Raised when required configuration is missing or invalid."""


@dataclass(frozen=True)
class Config:
    """All runtime configuration, sourced from environment variables."""

    source_db_url: URL
    dest_db_url: URL
    openweather_api_key: str
    openweather_base_url: str
    request_timeout_seconds: int
    rate_limit_calls_per_minute: int
    sql_file_path: Path
    create_table_sql_path: Path

    @classmethod
    def from_env(cls) -> "Config":
        load_dotenv()

        def require(name: str) -> str:
            value = os.getenv(name)
            if not value:
                raise PipelineConfigError(f"Missing required environment variable: {name}")
            return value

        # URL.create (rather than an f-string) percent-encodes each component,
        # so credentials containing '@', ':', or '/' don't break the connection string.
        source_db_url = URL.create(
            drivername="postgresql+psycopg2",
            username=require("SOURCE_DB_USER"),
            password=require("SOURCE_DB_PASSWORD"),
            host=require("SOURCE_DB_HOST"),
            port=int(require("SOURCE_DB_PORT")),
            database=require("SOURCE_DB_NAME"),
        )
        dest_db_url = URL.create(
            drivername="postgresql+psycopg2",
            username=require("DEST_DB_USER"),
            password=require("DEST_DB_PASSWORD"),
            host=require("DEST_DB_HOST"),
            port=int(require("DEST_DB_PORT")),
            database=require("DEST_DB_NAME"),
        )

        return cls(
            source_db_url=source_db_url,
            dest_db_url=dest_db_url,
            openweather_api_key=require("OPENWEATHER_API_KEY"),
            openweather_base_url=os.getenv("OPENWEATHER_BASE_URL", DEFAULT_OPENWEATHER_BASE_URL),
            request_timeout_seconds=int(
                os.getenv("REQUEST_TIMEOUT_SECONDS", DEFAULT_REQUEST_TIMEOUT_SECONDS)
            ),
            rate_limit_calls_per_minute=int(
                os.getenv("RATE_LIMIT_CALLS_PER_MINUTE", DEFAULT_RATE_LIMIT_CALLS_PER_MINUTE)
            ),
            sql_file_path=Path(os.getenv("SQL_FILE_PATH", DEFAULT_SQL_FILE_PATH)),
            create_table_sql_path=Path(
                os.getenv("CREATE_TABLE_SQL_PATH", DEFAULT_CREATE_TABLE_SQL_PATH)
            ),
        )


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )


# --------------------------------------------------------------------------
# Extract  (assignment Instruction 1: select postal codes via a .sql file,
# executed here through SQLAlchemy rather than a hardcoded query string)
# --------------------------------------------------------------------------

def build_engine(db_url: str) -> Engine:
    """Create a pooled SQLAlchemy engine. pool_pre_ping avoids stale connections
    on long-running/scheduled invocations."""
    return create_engine(db_url, pool_pre_ping=True)


def read_sql_file(path: Path) -> str:
    if not path.exists():
        raise PipelineConfigError(f"SQL file not found: {path}")
    sql = path.read_text(encoding="utf-8").strip()
    if not sql:
        raise PipelineConfigError(f"SQL file is empty: {path}")
    return sql


def extract_postal_codes(source_engine: Engine, sql_file_path: Path) -> pd.DataFrame:
    """Run the extraction query against the source database (mmai_db).

    Reading the query from a file rather than inlining it means this
    function is identical whether the query returns 15 rows or all
    postal codes in Canada.
    """
    query = read_sql_file(sql_file_path)
    logger.info("Extracting postal codes using %s", sql_file_path)

    with source_engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    missing_columns = set(REQUIRED_POSTAL_CODE_COLUMNS) - set(df.columns)
    if missing_columns:
        raise PipelineConfigError(
            f"Extraction query is missing required columns: {sorted(missing_columns)}"
        )

    before = len(df)
    df = df.dropna(subset=["postal_code", "latitude", "longitude"])
    df = df.drop_duplicates(subset=["postal_code"])
    dropped = before - len(df)
    if dropped:
        logger.warning("Dropped %d postal code row(s) with missing/duplicate data", dropped)

    logger.info("Extracted %d postal code(s)", len(df))
    return df


# --------------------------------------------------------------------------
# Transform  (assignment Instruction 4: data cleaning and feature engineering
# -- timestamp conversion, unit/precision cleanup, missing-field handling,
# and text normalization happen in transform_weather_record below)
# --------------------------------------------------------------------------

class TransientAPIError(RuntimeError):
    """Retryable network/5xx/429 error from the weather API."""


@retry(
    retry=retry_if_exception_type(TransientAPIError),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(5),
    reraise=True,
)
def fetch_current_weather(
    latitude: float,
    longitude: float,
    config: Config,
) -> dict[str, Any]:
    """Fetch current weather for a coordinate pair. Retries transient failures
    (timeouts, connection errors, 429, 5xx) with exponential backoff; does
    NOT retry on client errors like 401/404, which won't succeed on retry."""
    params = {
        "lat": latitude,
        "lon": longitude,
        "appid": config.openweather_api_key,
        "units": "metric",
    }
    try:
        response = requests.get(
            config.openweather_base_url,
            params=params,
            timeout=config.request_timeout_seconds,
        )
    except (requests.ConnectionError, requests.Timeout) as exc:
        raise TransientAPIError(str(exc)) from exc

    if response.status_code == 429 or response.status_code >= 500:
        raise TransientAPIError(f"HTTP {response.status_code} from weather API")

    response.raise_for_status()  # non-retryable 4xx (e.g. 401 bad key, 404) -> propagates
    return response.json()


def temp_category(temp_celsius: float | None) -> str | None:
    """Feature engineering (assignment Instruction 4): bucket a continuous
    temperature reading into a categorical range useful for downstream
    filtering/reporting (e.g. route-risk rules), distinct from the cleaning
    steps below."""
    if temp_celsius is None:
        return None
    if temp_celsius < 0:
        return "freezing"
    if temp_celsius < 10:
        return "cold"
    if temp_celsius < 20:
        return "mild"
    if temp_celsius < 28:
        return "warm"
    return "hot"


def transform_weather_record(
    postal_row: pd.Series, raw_weather: dict[str, Any]
) -> dict[str, Any] | None:
    """Clean a raw OpenWeather JSON payload, engineer a temp_category
    feature, and merge the result with location data into a single flat
    record ready for loading. Returns None (and logs a warning) if required
    fields are missing rather than raising, so one bad payload doesn't abort
    the whole pipeline run.
    """
    main = raw_weather.get("main") or {}
    weather_list = raw_weather.get("weather") or []
    epoch_seconds = raw_weather.get("dt")

    if not main or epoch_seconds is None:
        logger.warning(
            "Skipping postal code %s: incomplete weather payload", postal_row["postal_code"]
        )
        return None

    description = (weather_list[0].get("description", "") if weather_list else "").strip()
    temperature_celsius = _round_or_none(main.get("temp"))

    return {
        "province": postal_row["province"],
        "region": postal_row["region"],
        "postal_code": postal_row["postal_code"],
        "latitude": round(float(postal_row["latitude"]), 6),
        "longitude": round(float(postal_row["longitude"]), 6),
        "temperature_celsius": temperature_celsius,
        "feels_like_celsius": _round_or_none(main.get("feels_like")),
        "pressure_hpa": _int_or_none(main.get("pressure")),
        "humidity_pct": _int_or_none(main.get("humidity")),
        "weather_description": description.title() if description else None,
        "temp_category": temp_category(temperature_celsius),
        "measurement_timestamp": datetime.fromtimestamp(epoch_seconds, tz=timezone.utc),
        "pipeline_run_timestamp": datetime.now(timezone.utc),
    }


def _round_or_none(value: Any, ndigits: int = 2) -> float | None:
    return round(float(value), ndigits) if value is not None else None


def _int_or_none(value: Any) -> int | None:
    return int(value) if value is not None else None


class RateLimiter:
    """Simple sleep-based rate limiter to stay under an API's calls/minute quota."""

    def __init__(self, calls_per_minute: int) -> None:
        self._min_interval_seconds = 60.0 / max(calls_per_minute, 1)
        self._last_call_at: float | None = None

    def wait(self) -> None:
        if self._last_call_at is not None:
            elapsed = time.monotonic() - self._last_call_at
            remaining = self._min_interval_seconds - elapsed
            if remaining > 0:
                time.sleep(remaining)
        self._last_call_at = time.monotonic()


def build_weather_records(
    postal_codes: pd.DataFrame, config: Config
) -> list[dict[str, Any]]:
    """Fetch and transform weather for every postal code. Isolates failures
    per row so a single postal code's API error doesn't abort the batch --
    essential once this scales to thousands of postal codes."""
    limiter = RateLimiter(config.rate_limit_calls_per_minute)
    records: list[dict[str, Any]] = []

    for _, postal_row in postal_codes.iterrows():
        postal_code = postal_row["postal_code"]
        limiter.wait()
        try:
            raw_weather = fetch_current_weather(
                latitude=postal_row["latitude"],
                longitude=postal_row["longitude"],
                config=config,
            )
        except Exception:
            logger.exception("Failed to fetch weather for postal code %s", postal_code)
            continue

        record = transform_weather_record(postal_row, raw_weather)
        if record is not None:
            records.append(record)

    logger.info("Transformed %d/%d postal code(s) into weather records", len(records), len(postal_codes))
    return records


# --------------------------------------------------------------------------
# Load  (assignment Instruction 5: create/populate the uploads.current_weather
# table -- location data, weather metrics, and both required timestamps)
# --------------------------------------------------------------------------

def ensure_destination_table(dest_engine: Engine, create_table_sql_path: Path) -> None:
    """Idempotently create the uploads schema/table on the destination
    database so the pipeline is self-provisioning across environments."""
    ddl = read_sql_file(create_table_sql_path)
    with dest_engine.begin() as conn:
        for statement in filter(None, (s.strip() for s in ddl.split(";"))):
            conn.execute(text(statement))
    logger.info("Verified destination table %s exists", TARGET_TABLE)


def load_weather_records(dest_engine: Engine, records: list[dict[str, Any]]) -> int:
    """Upsert records into uploads.current_weather. ON CONFLICT DO NOTHING on
    the (postal_code, measurement_timestamp) primary key makes repeated or
    scheduled runs idempotent without duplicating or overwriting existing
    rows, per the assignment's requirement."""
    if not records:
        logger.info("No records to load")
        return 0

    from sqlalchemy import MetaData, Table

    metadata = MetaData(schema="uploads")
    current_weather = Table("current_weather", metadata, autoload_with=dest_engine)

    with dest_engine.begin() as conn:
        stmt = pg_insert(current_weather).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=["postal_code", "measurement_timestamp"])
        result = conn.execute(stmt)

    inserted = result.rowcount if result.rowcount is not None else len(records)
    logger.info("Inserted %d new row(s) into %s (%d skipped as duplicates)", inserted, TARGET_TABLE, len(records) - inserted)
    return inserted


# --------------------------------------------------------------------------
# Orchestration  (assignment Instruction 3: pipeline is safe to schedule daily
# -- idempotent load, no destructive writes -- and scales unchanged from the
# 15-postal-code sample to all of Canada, since row count only affects how
# many extract/transform iterations run)
# --------------------------------------------------------------------------

def run_pipeline(config: Config, dry_run: bool = False) -> None:
    source_engine = build_engine(config.source_db_url)
    dest_engine = build_engine(config.dest_db_url)

    try:
        postal_codes = extract_postal_codes(source_engine, config.sql_file_path)
        if postal_codes.empty:
            logger.warning("No postal codes returned by extraction query; nothing to do")
            return

        records = build_weather_records(postal_codes, config)

        if dry_run:
            logger.info("Dry run: would load %d record(s), skipping database write", len(records))
            return

        ensure_destination_table(dest_engine, config.create_table_sql_path)
        load_weather_records(dest_engine, records)
    except SQLAlchemyError:
        logger.exception("Database error during pipeline run")
        raise
    finally:
        source_engine.dispose()
        dest_engine.dispose()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sql-file",
        type=Path,
        default=None,
        help=f"Path to extraction SQL file (default: {DEFAULT_SQL_FILE_PATH} or $SQL_FILE_PATH)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and transform weather data but do not write to the database",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    setup_logging(args.log_level)

    try:
        config = Config.from_env()
    except PipelineConfigError as exc:
        logger.error("Configuration error: %s", exc)
        return 1

    if args.sql_file is not None:
        config = Config(**{**config.__dict__, "sql_file_path": args.sql_file})

    try:
        run_pipeline(config, dry_run=args.dry_run)
    except Exception:
        logger.exception("Pipeline run failed")
        return 1

    logger.info("Pipeline run completed successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())
