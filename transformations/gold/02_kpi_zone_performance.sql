-- =============================================================
-- Gold Layer: kpi_zone_performance
-- Monthly revenue and trip volume per pickup zone.
-- Enables geographic analysis and zone ranking.
--
-- Grain: one row per (month, zone)
-- Parameters: @execution_date STRING (YYYY-MM)
-- Target: nyc_taxi_{env}_gold.kpi_zone_performance
-- Partitioned by: month
-- =============================================================

CREATE OR REPLACE TABLE `{project_id}.{dataset_gold}`.kpi_zone_performance
PARTITION BY month
OPTIONS (
  description = 'Monthly performance metrics per taxi pickup zone. Used for geo analysis and ranking.',
  labels = [('layer', 'gold'), ('kpi', 'zones')]
)
AS

WITH

zone_metrics AS (
  SELECT
    DATE_TRUNC(t.pickup_date, MONTH)  AS month,
    t.pickup_location_id              AS location_id,
    z.zone                            AS zone_name,
    z.borough                         AS borough,
    z.borough_group,
    z.is_airport,

    -- Volume
    COUNT(*)                          AS total_pickups,
    SUM(t.passenger_count)            AS total_passengers,

    -- Revenue
    ROUND(SUM(t.total_amount), 2)     AS total_revenue,
    ROUND(AVG(t.total_amount), 2)     AS avg_revenue_per_trip,
    ROUND(SUM(t.fare_amount), 2)      AS total_fare,
    ROUND(SUM(t.tip_amount), 2)       AS total_tips,

    -- Trip quality
    ROUND(AVG(t.trip_distance_miles), 2) AS avg_distance_miles,
    ROUND(AVG(t.trip_duration_min), 1)   AS avg_duration_min,

    -- Tip behavior
    ROUND(AVG(t.tip_rate), 4)            AS avg_tip_rate,
    COUNTIF(t.payment_type = 'Credit Card') AS credit_card_trips,
    ROUND(SAFE_DIVIDE(
      COUNTIF(t.payment_type = 'Credit Card'), COUNT(*)
    ), 4)                                AS credit_card_rate

  FROM `{project_id}.{dataset_silver}`.trips t
  LEFT JOIN `{project_id}.{dataset_silver}`.dim_zones z
    ON t.pickup_location_id = z.location_id
  WHERE
    FORMAT_DATE('%Y-%m', t.pickup_date) = @execution_date
  GROUP BY
    1, 2, 3, 4, 5, 6
),

-- ── Compute zone rankings within each borough ────────────────
ranked AS (
  SELECT
    *,
    RANK() OVER (
      PARTITION BY month, borough
      ORDER BY total_revenue DESC
    ) AS rank_in_borough_by_revenue,

    RANK() OVER (
      PARTITION BY month
      ORDER BY total_pickups DESC
    ) AS rank_overall_by_pickups,

    -- Revenue share within borough
    ROUND(SAFE_DIVIDE(
      total_revenue,
      SUM(total_revenue) OVER (PARTITION BY month, borough)
    ), 4) AS revenue_share_in_borough,

    -- Month-over-month placeholders (populated in downstream views)
    CURRENT_TIMESTAMP() AS updated_at,
    @execution_date     AS _execution_date

  FROM zone_metrics
)

SELECT * FROM ranked
ORDER BY month, borough, rank_in_borough_by_revenue
;
