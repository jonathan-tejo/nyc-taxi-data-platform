-- =============================================================
-- Gold Layer: kpi_daily_revenue
-- Daily revenue and trip volume metrics.
-- Primary KPI table for executive dashboards.
--
-- Grain: one row per calendar day
-- Parameters: @execution_date STRING (YYYY-MM)
-- Target: nyc_taxi_{env}_gold.kpi_daily_revenue
-- Partitioned by: pickup_date
-- =============================================================

CREATE OR REPLACE TABLE `{project_id}.{dataset_gold}`.kpi_daily_revenue
PARTITION BY pickup_date
OPTIONS (
  description = 'Daily revenue and trip KPIs for NYC Yellow Taxi. One row per day.',
  labels = [('layer', 'gold'), ('kpi', 'revenue')]
)
AS

SELECT
  pickup_date,

  -- Volume metrics
  COUNT(*)                                                    AS total_trips,
  SUM(passenger_count)                                        AS total_passengers,
  ROUND(AVG(passenger_count), 2)                              AS avg_passengers_per_trip,

  -- Revenue metrics
  ROUND(SUM(total_amount), 2)                                 AS total_revenue,
  ROUND(AVG(total_amount), 2)                                 AS avg_revenue_per_trip,
  ROUND(SUM(fare_amount), 2)                                  AS total_fare,
  ROUND(SUM(tip_amount), 2)                                   AS total_tips,
  ROUND(SUM(tolls_amount), 2)                                 AS total_tolls,
  ROUND(SUM(congestion_surcharge), 2)                         AS total_congestion_surcharge,
  ROUND(SUM(airport_fee), 2)                                  AS total_airport_fees,

  -- Tip metrics
  ROUND(AVG(tip_rate), 4)                                     AS avg_tip_rate,
  ROUND(SAFE_DIVIDE(SUM(tip_amount), SUM(fare_amount)), 4)    AS overall_tip_rate,
  COUNTIF(tip_amount > 0)                                     AS trips_with_tip,
  ROUND(SAFE_DIVIDE(COUNTIF(tip_amount > 0), COUNT(*)), 4)    AS tip_incidence_rate,

  -- Trip quality metrics
  ROUND(AVG(trip_distance_miles), 2)                          AS avg_trip_distance_miles,
  ROUND(AVG(trip_duration_min), 1)                            AS avg_trip_duration_min,
  ROUND(SAFE_DIVIDE(SUM(trip_distance_miles), COUNT(*)), 2)   AS avg_distance_per_trip,

  -- Payment mix
  COUNTIF(payment_type = 'Credit Card')                       AS credit_card_trips,
  COUNTIF(payment_type = 'Cash')                              AS cash_trips,
  COUNTIF(payment_type = 'No Charge')                         AS no_charge_trips,
  ROUND(SAFE_DIVIDE(COUNTIF(payment_type = 'Credit Card'), COUNT(*)), 4)
                                                              AS credit_card_rate,

  -- Airport trips
  COUNTIF(is_airport_trip)                                    AS airport_trips,
  ROUND(SAFE_DIVIDE(COUNTIF(is_airport_trip), COUNT(*)), 4)   AS airport_trip_rate,

  -- Revenue efficiency
  ROUND(SAFE_DIVIDE(SUM(total_amount), NULLIF(SUM(trip_distance_miles), 0)), 2)
                                                              AS revenue_per_mile,
  ROUND(SAFE_DIVIDE(SUM(total_amount), NULLIF(SUM(trip_duration_min), 0)), 4)
                                                              AS revenue_per_minute,

  -- Percentile revenue (for outlier detection in dashboards)
  ROUND(APPROX_QUANTILES(total_amount, 100)[OFFSET(50)], 2)  AS p50_revenue,
  ROUND(APPROX_QUANTILES(total_amount, 100)[OFFSET(90)], 2)  AS p90_revenue,
  ROUND(APPROX_QUANTILES(total_amount, 100)[OFFSET(99)], 2)  AS p99_revenue,

  -- Metadata
  CURRENT_TIMESTAMP()                                         AS updated_at,
  @execution_date                                             AS _execution_date

FROM `{project_id}.{dataset_silver}`.trips
WHERE
  FORMAT_DATE('%Y-%m', pickup_date) = @execution_date

GROUP BY
  pickup_date

ORDER BY
  pickup_date
;
