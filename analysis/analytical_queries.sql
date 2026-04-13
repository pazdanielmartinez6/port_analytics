-- =============================================================
-- PORT OPERATIONS ANALYTICS — Sample Analytical Queries
-- =============================================================
-- These queries answer the five business questions from the
-- project brief and are designed to run directly against the
-- mart views without additional joins.
--
-- Run in psql:  \i analysis/analytical_queries.sql
-- =============================================================


-- ─────────────────────────────────────────────────────────────
-- Q1: Which ports handle the most containers and total cargo?
--     Annual comparison across all three ports, all years.
-- ─────────────────────────────────────────────────────────────
\echo '=== Q1: Annual port throughput comparison ==='

SELECT
    port_name,
    year,
    total_calls,
    total_teu,
    ROUND(total_teu / 1000.0, 1)                          AS total_teu_k,
    ROUND(total_weight_tonnes / 1_000_000.0, 2)           AS total_weight_mt,
    ROUND(total_cargo_value_usd / 1_000_000_000.0, 2)     AS total_value_bln_usd,
    delay_rate_pct,
    avg_turnaround_hours
FROM mart_port_comparison_annual
ORDER BY year, total_teu DESC;


-- ─────────────────────────────────────────────────────────────
-- Q2: Which terminals have the best turnaround time?
--     3-year average, ranked within each port.
-- ─────────────────────────────────────────────────────────────
\echo '=== Q2: Terminal efficiency ranking ==='

SELECT
    port_name,
    terminal_name,
    terminal_type,
    crane_count,
    SUM(total_calls)                                       AS total_calls,
    ROUND(AVG(avg_turnaround_hours), 1)                    AS avg_turnaround_hours,
    ROUND(AVG(avg_waiting_hours), 1)                       AS avg_waiting_hours,
    ROUND(AVG(avg_berth_hours), 1)                         AS avg_berth_hours,
    ROUND(AVG(crane_moves_per_crane_per_hour), 2)          AS avg_crane_productivity,
    ROUND(AVG(delay_rate_pct), 1)                          AS avg_delay_rate_pct,
    RANK() OVER (
        PARTITION BY port_name
        ORDER BY AVG(avg_turnaround_hours)
    )                                                      AS efficiency_rank
FROM mart_terminal_performance
GROUP BY port_name, terminal_name, terminal_type, crane_count
ORDER BY port_name, avg_turnaround_hours;


-- ─────────────────────────────────────────────────────────────
-- Q3: How do vessel size and carrier affect dwell time?
--     Focus on container ships; group by size class.
-- ─────────────────────────────────────────────────────────────
\echo '=== Q3: Dwell time by vessel size class and carrier alliance ==='

SELECT
    ship_size_class,
    alliance,
    COUNT(*)                                               AS total_calls,
    ROUND(AVG(avg_berth_hours),        1)                  AS avg_berth_hours,
    ROUND(AVG(avg_turnaround_hours),   1)                  AS avg_turnaround_hours,
    ROUND(AVG(avg_waiting_hours),      1)                  AS avg_waiting_hours,
    ROUND(AVG(delay_rate_pct),         1)                  AS avg_delay_rate_pct,
    ROUND(AVG(avg_teu_per_call),       0)                  AS avg_teu_per_call
FROM mart_vessel_performance
GROUP BY ship_size_class, alliance
ORDER BY avg_turnaround_hours;


-- ─────────────────────────────────────────────────────────────
-- Q4: Which routes show congestion or delay patterns?
--     Top 10 most congested route × port combinations
--     (congestion index = waiting time / route transit time).
-- ─────────────────────────────────────────────────────────────
\echo '=== Q4: Top 10 most congested route-port combinations ==='

SELECT
    port_name,
    route_name,
    trade_lane,
    SUM(movement_count)                                    AS total_movements,
    SUM(total_teu)                                         AS total_teu,
    ROUND(AVG(avg_waiting_hours),     1)                   AS avg_waiting_hours,
    ROUND(AVG(avg_turnaround_hours),  1)                   AS avg_turnaround_hours,
    ROUND(AVG(delay_rate_pct),        1)                   AS avg_delay_rate_pct,
    ROUND(AVG(congestion_index_pct),  2)                   AS avg_congestion_index
FROM mart_route_analysis
GROUP BY port_name, route_name, trade_lane
ORDER BY avg_congestion_index DESC
LIMIT 10;


-- ─────────────────────────────────────────────────────────────
-- Q5: How do port activity patterns differ by month/quarter?
--     Seasonality index: actual month TEU vs. annual average.
-- ─────────────────────────────────────────────────────────────
\echo '=== Q5: Seasonality — monthly TEU vs annual average ==='

WITH monthly AS (
    SELECT
        port_name,
        year,
        month,
        month_name,
        quarter,
        total_teu,
        total_calls
    FROM mart_port_monthly_throughput
    WHERE total_calls > 0
),
annual_avg AS (
    SELECT
        port_name,
        year,
        AVG(total_teu)   AS avg_monthly_teu,
        AVG(total_calls) AS avg_monthly_calls
    FROM monthly
    GROUP BY port_name, year
)
SELECT
    m.port_name,
    m.year,
    m.month,
    m.month_name,
    m.quarter,
    m.total_teu,
    m.total_calls,
    ROUND(m.total_teu::NUMERIC /
          NULLIF(a.avg_monthly_teu, 0) * 100, 1)           AS seasonality_index
FROM monthly m
JOIN annual_avg a USING (port_name, year)
ORDER BY m.port_name, m.year, m.month;


-- ─────────────────────────────────────────────────────────────
-- BONUS: Carrier market share per port (latest year)
-- ─────────────────────────────────────────────────────────────
\echo '=== BONUS: Carrier market share (2024) ==='

SELECT
    port_name,
    carrier_name,
    alliance,
    carrier_teu,
    carrier_calls,
    teu_share_pct,
    call_share_pct
FROM mart_carrier_market_share
WHERE year = 2024
ORDER BY port_name, teu_share_pct DESC;


-- ─────────────────────────────────────────────────────────────
-- BONUS: Import / Export balance by port and year
-- ─────────────────────────────────────────────────────────────
\echo '=== BONUS: Trade balance (import vs export TEU) ==='

SELECT
    port_name,
    year,
    SUM(import_teu)                                        AS annual_import_teu,
    SUM(export_teu)                                        AS annual_export_teu,
    SUM(transshipment_teu)                                 AS annual_transshipment_teu,
    ROUND(
        SUM(import_teu)::NUMERIC /
        NULLIF(SUM(export_teu), 0), 2
    )                                                      AS import_export_ratio
FROM mart_port_monthly_throughput
WHERE total_calls > 0
GROUP BY port_name, year
ORDER BY port_name, year;
