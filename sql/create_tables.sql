-- Assignment Instructions 2 & 5 (Create a Schema / Create Tables): creates
-- the destination schema/table for ingested weather data, with columns for
-- location data, weather metrics, and both required timestamps.
-- Runs against the DESTINATION database (your personal db), NOT mmai_db.
--
-- Idempotent: safe to run multiple times. The pipeline also executes this
-- file automatically on every run, so the destination is self-provisioning
-- and does not depend on a manual DataGrip step being remembered.
--
-- The composite primary key (postal_code, measurement_timestamp) is what
-- makes loads idempotent: re-running the pipeline for a postal code/time
-- that's already stored is a no-op (see ON CONFLICT DO NOTHING in the
-- Python loader) instead of a duplicate row or an overwrite.
CREATE SCHEMA IF NOT EXISTS uploads;

CREATE TABLE IF NOT EXISTS uploads.current_weather (
    -- Location data
    province                VARCHAR(50)   NOT NULL,
    region                  VARCHAR(100)  NOT NULL,
    postal_code             VARCHAR(10)   NOT NULL,
    latitude                NUMERIC(9, 6) NOT NULL,
    longitude               NUMERIC(9, 6) NOT NULL,

    -- Weather metrics
    temperature_celsius     NUMERIC(5, 2),
    feels_like_celsius      NUMERIC(5, 2),
    pressure_hpa            INTEGER,
    humidity_pct            INTEGER,
    weather_description     VARCHAR(255),

    -- Feature engineering (Instruction 4): temperature bucketed into a
    -- categorical range (freezing/cold/mild/warm/hot), derived from
    -- temperature_celsius rather than pulled directly from the API.
    temp_category            VARCHAR(20),

    -- Timestamps
    measurement_timestamp   TIMESTAMPTZ NOT NULL,           -- when the weather was measured (from API)
    pipeline_run_timestamp  TIMESTAMPTZ NOT NULL DEFAULT now(),  -- when this pipeline run captured the row

    PRIMARY KEY (postal_code, measurement_timestamp)
);

CREATE INDEX IF NOT EXISTS idx_current_weather_measurement_timestamp
    ON uploads.current_weather (measurement_timestamp);
