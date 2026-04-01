-- =============================================================
-- Gold Layer: kpi_hourly_patterns
-- Average trip metrics by day-of-week × hour-of-day.
-- Useful for demand forecasting and surge pricing analysis.
--
-- Grain: one row per (month, day_of_week, hour)
-- Parameters: @execution_date STRING (YYYY-MM)
-- Target: nyc_taxi_{env}_gold.kpi_hourly_patterns
-- =============================================================

CREATE OR REPLACE TABLE `{project_id}.{dataset_gold}`.kpi_hourly_patterns
OPTIONS (
  description = 'Trip demand and revenue patterns by hour of day and day of week.',
  labels = [('layer', 'gold'), ('kpi', 'demand')]
)
AS

WITH

hourly_stats AS (
  SELECT
    FORMAT_DATE('%Y-%m', pickup_date) AS month,
    pickup_dow,
    CASE pickup_dow
      WHEN 1 THEN 'Sunday'    WHEN 2 THEN 'Monday'
      WHEN 3 THEN 'Tuesday'   WHEN 4 THEN 'Wednesday'
      WHEN 5 THEN 'Thursday'  WHEN 6 THEN 'Friday'
      WHEN 7 THEN 'Saturday'
    END                               AS day_of_week,
    CASE
      WHEN pickup_dow IN (1, 7) THEN 'Weekend'
      ELSE 'Weekday'
    END                               AS day_type,
    pickup_hour,

    -- Demand
    COUNT(*)                                                  AS total_trips,
    ROUND(COUNT(*) / COUNT(DISTINCT pickup_date), 1)          AS avg_trips_per_day,

    -- Revenue
    ROUND(AVG(total_amount), 2)                               AS avg_revenue,
    ROUND(SUM(total_amount), 2)                               AS total_revenue,
    ROUND(APPROX_QUANTILES(total_amount, 100)[OFFSET(50)], 2) AS median_revenue,

    -- Trip characteristics
    ROUND(AVG(trip_distance_miles), 2)                        AS avg_distance_miles,
    ROUND(AVG(trip_duration_min), 1)                          AS avg_duration_min,
    ROUND(AVG(passenger_count), 2)                            AS avg_passengers,

    -- Tip behavior
    ROUND(AVG(tip_rate), 4)                                   AS avg_tip_rate,
    ROUND(SAFE_DIVIDE(COUNTIF(tip_amount > 0), COUNT(*)), 4)  AS tip_incidence_rate,

    -- Payment
    ROUND(SAFE_DIVIDE(COUNTIF(payment_type = 'Credit Card'), COUNT(*)), 4)
                                                              AS credit_card_rate,

    CURRENT_TIMESTAMP() AS updated_at

  FROM `{project_id}.{dataset_silver}`.trips
  WHERE
    FORMAT_DATE('%Y-%m', pickup_date) = @execution_date
  GROUP BY
    1, 2, 3, 4, 5
)

SELECT
  *,
  -- Peak hour classification
  CASE
    WHEN pickup_hour BETWEEN 7  AND 9  THEN 'Morning Rush'
    WHEN pickup_hour BETWEEN 11 AND 13 THEN 'Lunch'
    WHEN pickup_hour BETWEEN 17 AND 19 THEN 'Evening Rush'
    WHEN pickup_hour BETWEEN 22 AND 23 OR pickup_hour = 0 THEN 'Late Night'
    WHEN pickup_hour BETWEEN 1  AND 5  THEN 'Overnight'
    ELSE 'Off-Peak'
  END AS time_of_day_segment,

  -- Relative demand index (vs. overall avg for this month)
  ROUND(SAFE_DIVIDE(
    avg_trips_per_day,
    AVG(avg_trips_per_day) OVER ()
  ), 3) AS demand_index

FROM hourly_stats
ORDER BY
  month, pickup_dow, pickup_hour
;
