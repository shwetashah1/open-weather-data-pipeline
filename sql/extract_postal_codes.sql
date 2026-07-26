-- Assignment Instruction 1 (Select Postal Codes): extracts a representative
-- sample of Toronto postal codes for the weather ingestion pipeline.
-- Runs against the SOURCE database (mmai_db).
--
-- To scale this pipeline to all of Canada, remove the WHERE/LIMIT clauses
-- below (or parameterize `region`) -- the Python pipeline reads and executes
-- this file as-is and iterates over however many rows it returns, so no
-- code changes are required to go from 15 rows to the full country.
SELECT
    province,
    region,
    postal_code,
    latitude,
    longitude
FROM uploads.ca_postal_codes
WHERE region = 'Toronto'
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL
ORDER BY postal_code
LIMIT 15;
