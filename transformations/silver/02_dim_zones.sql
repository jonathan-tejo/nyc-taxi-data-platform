-- =============================================================
-- Silver Layer: dim_zones
-- Loads NYC TLC Taxi Zone lookup table as a dimension.
-- Source: BigQuery public dataset or GCS upload.
-- This is a static reference table — run once, not monthly.
--
-- Target: nyc_taxi_{env}_silver.dim_zones
-- =============================================================

CREATE OR REPLACE TABLE `{project_id}.{dataset_silver}`.dim_zones
OPTIONS (
  description = 'NYC TLC Taxi Zone lookup — maps LocationID to borough, zone name, and service zone',
  labels = [('layer', 'silver'), ('type', 'dimension')]
)
AS

SELECT
  LocationID        AS location_id,
  Borough           AS borough,
  Zone              AS zone,
  service_zone,

  -- Derived groupings for analytics
  CASE Borough
    WHEN 'Manhattan'   THEN 'Core Manhattan'
    WHEN 'Brooklyn'    THEN 'Outer Borough'
    WHEN 'Queens'      THEN 'Outer Borough'
    WHEN 'Bronx'       THEN 'Outer Borough'
    WHEN 'Staten Island' THEN 'Outer Borough'
    WHEN 'EWR'         THEN 'Airport'
    ELSE 'Unknown'
  END AS borough_group,

  -- Airport location flag
  LocationID IN (132, 138, 1) AS is_airport,

  CURRENT_TIMESTAMP() AS loaded_at

FROM `bigquery-public-data.new_york_taxi_trips.taxi_zone_geom`
-- Alternative if using uploaded CSV:
-- FROM `{project_id}.{dataset_bronze}`.taxi_zones_raw
;
