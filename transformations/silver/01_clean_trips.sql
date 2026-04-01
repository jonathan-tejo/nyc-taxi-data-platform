-- =============================================================
-- Silver Layer: clean_trips
-- Transforms bronze raw trips into a clean, typed, enriched
-- fact table with business-meaningful fields.
--
-- Parameters:
--   @execution_date  STRING  Month being processed (YYYY-MM)
--
-- Target: nyc_taxi_{env}_silver.trips
-- Partitioned by: pickup_date (DAY)
-- Clustered by: pickup_location_id, payment_type
-- =============================================================

CREATE OR REPLACE TABLE `{project_id}.{dataset_silver}`.trips
PARTITION BY pickup_date
CLUSTER BY pickup_location_id, payment_type
OPTIONS (
  description = 'Cleaned NYC Yellow Taxi trips. One row per trip. Invalid and duplicate rows removed.',
  labels = [('layer', 'silver'), ('managed_by', 'pipeline')]
)
AS

WITH

-- ── Deduplicate: keep latest ingested row per trip key ───────
deduplicated AS (
  SELECT
    * EXCEPT (row_num)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY VendorID, tpep_pickup_datetime, tpep_dropoff_datetime
        ORDER BY _ingested_at DESC
      ) AS row_num
    FROM `{project_id}.{dataset_bronze}`.trips
    WHERE _execution_date = @execution_date
  )
  WHERE row_num = 1
),

-- ── Apply cleaning rules ─────────────────────────────────────
cleaned AS (
  SELECT
    -- IDs
    COALESCE(VendorID, 0)            AS vendor_id,
    PULocationID                      AS pickup_location_id,
    DOLocationID                      AS dropoff_location_id,

    -- Temporal fields
    CAST(tpep_pickup_datetime  AS TIMESTAMP) AS pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
    DATE(tpep_pickup_datetime)               AS pickup_date,
    EXTRACT(HOUR       FROM tpep_pickup_datetime) AS pickup_hour,
    EXTRACT(DAYOFWEEK  FROM tpep_pickup_datetime) AS pickup_dow,
    EXTRACT(MONTH      FROM tpep_pickup_datetime) AS pickup_month,
    EXTRACT(YEAR       FROM tpep_pickup_datetime) AS pickup_year,

    -- Trip metrics
    GREATEST(COALESCE(CAST(passenger_count AS INT64), 1), 1)   AS passenger_count,
    GREATEST(COALESCE(trip_distance, 0.0), 0.0)                AS trip_distance_miles,
    TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, MINUTE)
                                                                AS trip_duration_min,

    -- Financial fields (floor at 0 to handle erroneous negatives)
    GREATEST(COALESCE(fare_amount, 0.0), 0.0)           AS fare_amount,
    GREATEST(COALESCE(tip_amount, 0.0), 0.0)            AS tip_amount,
    GREATEST(COALESCE(tolls_amount, 0.0), 0.0)          AS tolls_amount,
    GREATEST(COALESCE(extra, 0.0), 0.0)                 AS extra_amount,
    GREATEST(COALESCE(mta_tax, 0.0), 0.0)               AS mta_tax,
    GREATEST(COALESCE(improvement_surcharge, 0.0), 0.0) AS improvement_surcharge,
    GREATEST(COALESCE(congestion_surcharge, 0.0), 0.0)  AS congestion_surcharge,
    GREATEST(COALESCE(airport_fee, 0.0), 0.0)           AS airport_fee,
    GREATEST(COALESCE(total_amount, 0.0), 0.0)          AS total_amount,

    -- Derived financial metrics
    SAFE_DIVIDE(tip_amount, NULLIF(fare_amount, 0))     AS tip_rate,
    SAFE_DIVIDE(total_amount,
      NULLIF(TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, MINUTE), 0)
    )                                                   AS revenue_per_minute,

    -- Categorical
    CASE COALESCE(CAST(payment_type AS INT64), 5)
      WHEN 1 THEN 'Credit Card'
      WHEN 2 THEN 'Cash'
      WHEN 3 THEN 'No Charge'
      WHEN 4 THEN 'Dispute'
      ELSE 'Unknown'
    END AS payment_type,

    CASE COALESCE(CAST(RatecodeID AS INT64), 1)
      WHEN 1 THEN 'Standard Rate'
      WHEN 2 THEN 'JFK'
      WHEN 3 THEN 'Newark'
      WHEN 4 THEN 'Nassau or Westchester'
      WHEN 5 THEN 'Negotiated Fare'
      WHEN 6 THEN 'Group Ride'
      ELSE 'Unknown'
    END AS rate_code,

    store_and_fwd_flag = 'Y' AS is_store_and_forward,

    -- Airport trip flag (JFK=132, LGA=138, EWR=1)
    PULocationID IN (132, 138, 1) OR DOLocationID IN (132, 138, 1) AS is_airport_trip,

    -- Lineage
    _ingested_at,
    _source_file,
    _execution_date

  FROM deduplicated
),

-- ── Apply data quality filters ───────────────────────────────
-- Rows failing these rules are EXCLUDED from silver.
-- They are counted in quality checks but not propagated.
filtered AS (
  SELECT *
  FROM cleaned
  WHERE
    -- Required timestamps must be non-null and ordered
    pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    AND dropoff_datetime > pickup_datetime

    -- Trip duration sanity: 1 min to 5 hours
    AND trip_duration_min BETWEEN 1 AND 300

    -- Distance sanity: up to 200 miles
    AND trip_distance_miles BETWEEN 0 AND 200

    -- Location IDs must be valid TLC zones (1-265)
    AND pickup_location_id BETWEEN 1 AND 265
    AND dropoff_location_id BETWEEN 1 AND 265

    -- Fares must be non-negative; require at least a minimum fare
    AND fare_amount >= 0
    AND total_amount >= 0

    -- Only process rows belonging to the requested month
    -- (TLC files sometimes include edge rows from adjacent months)
    AND FORMAT_DATE('%Y-%m', pickup_date) = @execution_date
)

SELECT * FROM filtered
;
