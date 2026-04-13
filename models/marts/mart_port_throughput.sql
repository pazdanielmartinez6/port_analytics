-- =============================================================
-- models/marts/mart_port_throughput.sql
-- Purpose : Monthly port-level throughput mart.
--           Joins staging views to produce a wide, BI-ready table.
--           Run this SQL in psql to create/refresh the view.
-- =============================================================

CREATE OR REPLACE VIEW mart_port_throughput AS

WITH calls AS (
    SELECT
        s.port_key,
        s.year,
        s.month,
        COUNT(*)                                       AS total_calls,
        COUNT(*) FILTER (WHERE s.is_delayed = 1)       AS delayed_calls,
        ROUND(AVG(s.berth_waiting_hours)::NUMERIC, 2)  AS avg_waiting_hours,
        ROUND(AVG(s.at_berth_hours)::NUMERIC,      2)  AS avg_berth_hours,
        ROUND(AVG(s.turnaround_hours)::NUMERIC,    2)  AS avg_turnaround_hours,
        ROUND(AVG(s.crane_moves_per_hour)::NUMERIC,1)  AS avg_crane_moves_per_hour,
        COUNT(*) FILTER (WHERE s.turnaround_band_id = 1) AS fast_calls,
        COUNT(*) FILTER (WHERE s.turnaround_band_id = 2) AS normal_calls,
        COUNT(*) FILTER (WHERE s.turnaround_band_id = 3) AS slow_calls,
        COUNT(*) FILTER (WHERE s.turnaround_band_id = 4) AS very_slow_calls
    FROM stg_port_calls s
    GROUP BY s.port_key, s.year, s.month
),
cargo AS (
    SELECT
        s.port_key,
        EXTRACT(YEAR  FROM dd.full_date)::INT  AS year,
        EXTRACT(MONTH FROM dd.full_date)::INT  AS month,
        SUM(s.teu_count)                       AS total_teu,
        SUM(s.weight_tonnes)                   AS total_weight_tonnes,
        ROUND(SUM(s.cargo_value_usd)::NUMERIC, 0) AS total_cargo_value_usd,
        SUM(s.teu_count) FILTER (WHERE s.direction_key = 1) AS import_teu,
        SUM(s.teu_count) FILTER (WHERE s.direction_key = 2) AS export_teu,
        SUM(s.teu_count) FILTER (WHERE s.direction_key = 3) AS transshipment_teu,
        SUM(s.is_hazardous)                    AS hazardous_movements
    FROM stg_cargo_movements s
    JOIN dim_date dd ON dd.date_key = s.date_key
    GROUP BY s.port_key,
             EXTRACT(YEAR FROM dd.full_date),
             EXTRACT(MONTH FROM dd.full_date)
)
SELECT
    dp.port_name,
    dp.country_name,
    dp.continent,
    dp.port_type,
    dd_spine.year,
    dd_spine.month,
    dd_spine.month_name,
    dd_spine.quarter,

    -- Call metrics
    COALESCE(c.total_calls,     0)  AS total_calls,
    COALESCE(c.delayed_calls,   0)  AS delayed_calls,
    ROUND(
        COALESCE(c.delayed_calls, 0)::NUMERIC
        / NULLIF(c.total_calls, 0) * 100, 1
    )                               AS delay_rate_pct,

    COALESCE(c.avg_waiting_hours,       0) AS avg_waiting_hours,
    COALESCE(c.avg_berth_hours,         0) AS avg_berth_hours,
    COALESCE(c.avg_turnaround_hours,    0) AS avg_turnaround_hours,
    COALESCE(c.avg_crane_moves_per_hour,0) AS avg_crane_moves_per_hour,

    COALESCE(c.fast_calls,      0)  AS fast_calls,
    COALESCE(c.normal_calls,    0)  AS normal_calls,
    COALESCE(c.slow_calls,      0)  AS slow_calls,
    COALESCE(c.very_slow_calls, 0)  AS very_slow_calls,

    -- Throughput metrics
    COALESCE(g.total_teu,             0) AS total_teu,
    COALESCE(g.total_weight_tonnes,   0) AS total_weight_tonnes,
    COALESCE(g.total_cargo_value_usd, 0) AS total_cargo_value_usd,
    COALESCE(g.import_teu,            0) AS import_teu,
    COALESCE(g.export_teu,            0) AS export_teu,
    COALESCE(g.transshipment_teu,     0) AS transshipment_teu,
    COALESCE(g.hazardous_movements,   0) AS hazardous_movements,

    -- Imbalance ratio (import vs export balance)
    CASE
        WHEN COALESCE(g.export_teu, 0) > 0
        THEN ROUND(
            COALESCE(g.import_teu, 0)::NUMERIC
            / NULLIF(g.export_teu, 0), 2)
        ELSE NULL
    END                             AS import_export_ratio

FROM dim_port dp
CROSS JOIN (
    SELECT DISTINCT year, month, month_name, quarter
    FROM dim_date
) dd_spine
LEFT JOIN calls c ON c.port_key = dp.port_key
                  AND c.year = dd_spine.year
                  AND c.month = dd_spine.month
LEFT JOIN cargo g ON g.port_key = dp.port_key
                  AND g.year = dd_spine.year
                  AND g.month = dd_spine.month

ORDER BY dp.port_name, dd_spine.year, dd_spine.month;

COMMENT ON VIEW mart_port_throughput IS
    'Mart: monthly port throughput with call efficiency, cargo volume, '
    'and direction breakdown. Primary dashboard data source.';
