-- =============================================================
-- PORT OPERATIONS ANALYTICS WAREHOUSE
-- 04_views.sql  —  Dashboard-ready mart views  (v2 — fan-out fix)
--
-- CHANGE LOG vs v1
-- ─────────────────────────────────────────────────────────────
-- mart_port_monthly_throughput:
--   BUG: direct LEFT JOIN between fact_port_calls and
--        fact_cargo_movements before aggregation caused fan-out
--        inflation of every port-call KPI (avg_waiting_hours,
--        avg_berth_hours, avg_turnaround_hours, avg_crane_moves)
--        by the number of cargo-movement rows per call.
--   FIX: aggregate efficiency KPIs at port-call grain first in
--        a CTE, then LEFT JOIN to the cargo CTE. Cargo totals
--        are aggregated independently on fact_cargo_movements.
--
-- mart_port_comparison_annual:
--   Same fan-out bug; same two-CTE pattern applied.
--
-- All other views are unchanged from v1.
-- =============================================================

SET client_min_messages = WARNING;

-- ─────────────────────────────────────────────────────────────
-- mart_port_monthly_throughput
-- Grain: one row per port × year × month
--
-- FAN-OUT FIX: efficiency KPIs (avg_waiting_hours, etc.) are
-- computed at port-call grain in CTE `calls`, cargo totals
-- are computed independently in CTE `cargo`, and the two sets
-- are joined — never joined before aggregation.
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW mart_port_monthly_throughput AS

WITH calls AS (
    -- ── STEP 1: aggregate at port-call grain ─────────────
    -- Only fact_port_calls touched here — no cargo join.
    SELECT
        fpc.port_key,
        fpc.year,
        fpc.month,
        COUNT(*)                                            AS total_port_calls,
        COUNT(*) FILTER (WHERE fpc.is_delayed = 1)         AS delayed_calls,
        ROUND(AVG(fpc.berth_waiting_hours)::NUMERIC, 2)    AS avg_waiting_hours,
        ROUND(AVG(fpc.at_berth_hours)::NUMERIC,      2)    AS avg_berth_hours,
        ROUND(AVG(fpc.turnaround_hours)::NUMERIC,    2)    AS avg_turnaround_hours,
        ROUND(AVG(fpc.crane_moves_total)::NUMERIC,   0)    AS avg_crane_moves
    FROM fact_port_calls fpc
    GROUP BY fpc.port_key, fpc.year, fpc.month
),
cargo AS (
    -- ── STEP 2: aggregate cargo independently ─────────────
    -- No join to fact_port_calls — avoids fan-out entirely.
    SELECT
        fcm.port_key,
        EXTRACT(YEAR  FROM dd.full_date)::INT               AS year,
        EXTRACT(MONTH FROM dd.full_date)::INT               AS month,
        COALESCE(SUM(fcm.teu_count),       0)               AS total_teu,
        COALESCE(SUM(fcm.weight_tonnes),   0)               AS total_weight_tonnes,
        ROUND(COALESCE(SUM(fcm.cargo_value_usd), 0)::NUMERIC, 0)
                                                            AS total_cargo_value_usd,
        SUM(fcm.teu_count) FILTER (WHERE fcm.direction_key = 1)
                                                            AS import_teu,
        SUM(fcm.teu_count) FILTER (WHERE fcm.direction_key = 2)
                                                            AS export_teu,
        SUM(fcm.teu_count) FILTER (WHERE fcm.direction_key = 3)
                                                            AS transshipment_teu,
        SUM(fcm.is_hazardous)                               AS hazardous_movements
    FROM fact_cargo_movements fcm
    JOIN dim_date             dd ON dd.date_key = fcm.date_key
    GROUP BY fcm.port_key,
             EXTRACT(YEAR  FROM dd.full_date),
             EXTRACT(MONTH FROM dd.full_date)
)
-- ── STEP 3: join call KPIs + cargo totals to port + date spine ──
SELECT
    dp.port_name,
    dp.country_name,
    dp.continent,
    dp.port_type,
    dd_spine.year,
    dd_spine.month,
    dd_spine.month_name,
    dd_spine.quarter,

    -- Call metrics (sourced from calls CTE — no fan-out)
    COALESCE(c.total_port_calls, 0)                         AS total_port_calls,
    COALESCE(c.delayed_calls,    0)                         AS delayed_calls,
    ROUND(
        COALESCE(c.delayed_calls, 0)::NUMERIC
        / NULLIF(c.total_port_calls, 0) * 100, 1
    )                                                       AS delay_rate_pct,

    -- Efficiency KPIs (correctly averaged at call grain — no fan-out)
    COALESCE(c.avg_waiting_hours,    0)                     AS avg_waiting_hours,
    COALESCE(c.avg_berth_hours,      0)                     AS avg_berth_hours,
    COALESCE(c.avg_turnaround_hours, 0)                     AS avg_turnaround_hours,
    COALESCE(c.avg_crane_moves,      0)                     AS avg_crane_moves,

    -- Throughput metrics (sourced from cargo CTE — no fan-out)
    COALESCE(g.total_teu,             0)                    AS total_teu,
    COALESCE(g.total_weight_tonnes,   0)                    AS total_weight_tonnes,
    COALESCE(g.total_cargo_value_usd, 0)                    AS total_cargo_value_usd,
    COALESCE(g.import_teu,            0)                    AS import_teu,
    COALESCE(g.export_teu,            0)                    AS export_teu,
    COALESCE(g.transshipment_teu,     0)                    AS transshipment_teu,
    COALESCE(g.hazardous_movements,   0)                    AS hazardous_movements,

    -- Import/export balance ratio
    CASE
        WHEN COALESCE(g.export_teu, 0) > 0
        THEN ROUND(
            COALESCE(g.import_teu, 0)::NUMERIC
            / NULLIF(g.export_teu, 0), 2)
        ELSE NULL
    END                                                     AS import_export_ratio

FROM dim_port dp
CROSS JOIN (
    -- Date spine: one row per distinct year × month combination
    SELECT DISTINCT year, month, month_name, quarter
    FROM dim_date
) dd_spine
LEFT JOIN calls c
       ON c.port_key = dp.port_key
      AND c.year     = dd_spine.year
      AND c.month    = dd_spine.month
LEFT JOIN cargo g
       ON g.port_key = dp.port_key
      AND g.year     = dd_spine.year
      AND g.month    = dd_spine.month

ORDER BY dp.port_name, dd_spine.year, dd_spine.month;

COMMENT ON VIEW mart_port_monthly_throughput IS
    'Monthly port throughput + efficiency KPIs. '
    'Fan-out fixed (v2): call KPIs and cargo totals aggregated '
    'in separate CTEs before joining. Primary time-series dashboard view.';

-- ─────────────────────────────────────────────────────────────
-- mart_terminal_performance
-- Grain: one row per terminal × year × quarter
-- (Unchanged from v1 — no fan-out issue: counts use DISTINCT.)
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW mart_terminal_performance AS
WITH calls AS (
    SELECT
        fpc.terminal_key,
        fpc.port_key,
        fpc.year,
        (fpc.month - 1) / 3 + 1                            AS quarter,
        COUNT(*)                                            AS total_calls,
        COUNT(*) FILTER (WHERE fpc.is_delayed = 1)         AS delayed_calls,
        ROUND(AVG(fpc.berth_waiting_hours)::NUMERIC, 2)    AS avg_waiting_hours,
        ROUND(AVG(fpc.at_berth_hours)::NUMERIC,      2)    AS avg_berth_hours,
        ROUND(AVG(fpc.turnaround_hours)::NUMERIC,    2)    AS avg_turnaround_hours,
        ROUND(AVG(fpc.crane_moves_total)::NUMERIC,   0)    AS avg_crane_moves
    FROM fact_port_calls fpc
    GROUP BY fpc.terminal_key, fpc.port_key, fpc.year, (fpc.month - 1) / 3 + 1
),
cargo AS (
    SELECT
        fcm.terminal_key,
        EXTRACT(YEAR FROM dd.full_date)::INT                AS year,
        EXTRACT(QUARTER FROM dd.full_date)::INT             AS quarter,
        COALESCE(SUM(fcm.teu_count),     0)                 AS total_teu,
        COALESCE(SUM(fcm.weight_tonnes), 0)                 AS total_weight_tonnes
    FROM fact_cargo_movements fcm
    JOIN dim_date             dd ON dd.date_key = fcm.date_key
    GROUP BY fcm.terminal_key,
             EXTRACT(YEAR FROM dd.full_date),
             EXTRACT(QUARTER FROM dd.full_date)
)
SELECT
    dp.port_name,
    dt.terminal_name,
    dt.terminal_type,
    dt.crane_count,
    c.year,
    c.quarter,

    c.total_calls,
    COALESCE(g.total_teu,             0)                    AS total_teu,
    COALESCE(g.total_weight_tonnes,   0)                    AS total_weight_tonnes,

    c.avg_waiting_hours,
    c.avg_berth_hours,
    c.avg_turnaround_hours,
    c.avg_crane_moves,

    -- Crane productivity: moves per crane per hour
    ROUND(
        CASE
            WHEN dt.crane_count > 0 AND c.avg_berth_hours > 0
            THEN c.avg_crane_moves::NUMERIC / dt.crane_count / c.avg_berth_hours
            ELSE NULL
        END, 2
    )                                                       AS crane_moves_per_crane_per_hour,

    ROUND(c.delayed_calls::NUMERIC / NULLIF(c.total_calls, 0) * 100, 1)
                                                            AS delay_rate_pct

FROM calls        c
JOIN dim_terminal dt  ON dt.terminal_key = c.terminal_key
JOIN dim_port     dp  ON dp.port_key     = c.port_key
LEFT JOIN cargo   g   ON g.terminal_key  = c.terminal_key
                      AND g.year         = c.year
                      AND g.quarter      = c.quarter

ORDER BY dp.port_name, dt.terminal_name, c.year, c.quarter;

COMMENT ON VIEW mart_terminal_performance IS
    'Quarterly terminal-level efficiency including crane productivity. '
    'Two-CTE pattern prevents fan-out between fact tables.';

-- ─────────────────────────────────────────────────────────────
-- mart_vessel_dwell_analysis
-- Grain: one row per port call (flat, join-free for BI tools)
-- (Unchanged from v1 — no cargo join, so no fan-out risk.)
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW mart_vessel_dwell_analysis AS
SELECT
    fpc.port_call_id,
    fpc.arrival_datetime,
    fpc.departure_datetime,

    dp.port_name,
    dt.terminal_name,
    dt.terminal_type,
    dv.vessel_name,
    dv.vessel_type,
    dv.teu_capacity,
    dv.deadweight_tonnes,
    dss.class_name                                          AS ship_size_class,
    dc.carrier_name,
    dc.alliance,
    dr.route_name,
    dr.trade_lane,
    dd.year,
    dd.quarter,
    dd.month,
    dd.month_name,
    dd.season_name,          -- new: from dim_date enhancement
    dd.is_public_holiday,    -- new: from dim_date enhancement

    fpc.berth_waiting_hours,
    fpc.at_berth_hours,
    fpc.turnaround_hours,
    fpc.crane_moves_total,
    fpc.is_delayed,
    fpc.delay_hours,

    -- Crane productivity (moves per berth-hour)
    ROUND(fpc.crane_moves_total::NUMERIC /
          NULLIF(fpc.at_berth_hours, 0), 1)                 AS crane_moves_per_hour,

    -- Turnaround performance band
    CASE
        WHEN fpc.turnaround_hours < 24  THEN 'Fast (<24h)'
        WHEN fpc.turnaround_hours < 48  THEN 'Normal (24-48h)'
        WHEN fpc.turnaround_hours < 72  THEN 'Slow (48-72h)'
        ELSE                                 'Very Slow (>72h)'
    END                                                     AS turnaround_band

FROM fact_port_calls        fpc
JOIN dim_port               dp  ON dp.port_key            = fpc.port_key
JOIN dim_terminal           dt  ON dt.terminal_key        = fpc.terminal_key
JOIN dim_vessel             dv  ON dv.vessel_key          = fpc.vessel_key
JOIN dim_ship_size_class    dss ON dss.ship_size_class_key = fpc.ship_size_class_key
JOIN dim_carrier            dc  ON dc.carrier_key         = fpc.carrier_key
JOIN dim_route              dr  ON dr.route_key           = fpc.route_key
JOIN dim_date               dd  ON dd.date_key            = fpc.date_key;

COMMENT ON VIEW mart_vessel_dwell_analysis IS
    'Flat, join-free view of every port call. '
    'Enhanced (v2): season_name and is_public_holiday exposed from dim_date.';

-- ─────────────────────────────────────────────────────────────
-- mart_cargo_by_route_and_direction
-- Grain: port × route × direction × cargo type × year × quarter
-- (Unchanged from v1.)
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW mart_cargo_by_route_and_direction AS
SELECT
    dp.port_name,
    dp.country_name,
    dr.route_name,
    dr.trade_lane,
    ddir.direction_name,
    dct.cargo_type_name,
    dct.cargo_category,
    dd.year,
    dd.quarter,

    COUNT(*)                                                AS movement_count,
    SUM(fcm.teu_count)                                      AS total_teu,
    SUM(fcm.weight_tonnes)                                  AS total_weight_tonnes,
    ROUND(SUM(fcm.cargo_value_usd)::NUMERIC, 0)             AS total_cargo_value_usd,
    SUM(fcm.is_hazardous)                                   AS hazardous_movement_count

FROM fact_cargo_movements   fcm
JOIN dim_port               dp   ON dp.port_key         = fcm.port_key
JOIN dim_route              dr   ON dr.route_key         = fcm.route_key
JOIN dim_direction          ddir ON ddir.direction_key   = fcm.direction_key
JOIN dim_cargo_type         dct  ON dct.cargo_type_key   = fcm.cargo_type_key
JOIN dim_date               dd   ON dd.date_key          = fcm.date_key

GROUP BY
    dp.port_name, dp.country_name,
    dr.route_name, dr.trade_lane,
    ddir.direction_name,
    dct.cargo_type_name, dct.cargo_category,
    dd.year, dd.quarter

ORDER BY dp.port_name, dd.year, dd.quarter, total_teu DESC;

COMMENT ON VIEW mart_cargo_by_route_and_direction IS
    'Trade lane and direction breakdown of cargo throughput. '
    'Primary input for Cargo Operations dashboard page.';

-- ─────────────────────────────────────────────────────────────
-- mart_carrier_market_share
-- Grain: carrier × port × year
-- (Unchanged from v1.)
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW mart_carrier_market_share AS
WITH base AS (
    SELECT
        dp.port_name,
        dc.carrier_name,
        dc.alliance,
        dd.year,
        SUM(fcm.teu_count)                                  AS carrier_teu,
        SUM(fcm.weight_tonnes)                              AS carrier_weight_tonnes,
        COUNT(DISTINCT fpc.port_call_id)                    AS carrier_calls
    FROM fact_cargo_movements   fcm
    JOIN fact_port_calls        fpc ON fpc.port_call_id = fcm.port_call_id
    JOIN dim_port               dp  ON dp.port_key      = fpc.port_key
    JOIN dim_carrier            dc  ON dc.carrier_key   = fpc.carrier_key
    JOIN dim_date               dd  ON dd.date_key      = fpc.date_key
    GROUP BY dp.port_name, dc.carrier_name, dc.alliance, dd.year
),
port_totals AS (
    SELECT port_name, year,
           SUM(carrier_teu)   AS port_total_teu,
           SUM(carrier_calls) AS port_total_calls
    FROM base
    GROUP BY port_name, year
)
SELECT
    b.port_name,
    b.carrier_name,
    b.alliance,
    b.year,
    b.carrier_teu,
    b.carrier_weight_tonnes,
    b.carrier_calls,
    ROUND(b.carrier_teu::NUMERIC   / NULLIF(pt.port_total_teu,   0) * 100, 2) AS teu_share_pct,
    ROUND(b.carrier_calls::NUMERIC / NULLIF(pt.port_total_calls, 0) * 100, 2) AS call_share_pct
FROM base        b
JOIN port_totals pt ON pt.port_name = b.port_name AND pt.year = b.year
ORDER BY b.port_name, b.year, b.carrier_teu DESC;

COMMENT ON VIEW mart_carrier_market_share IS
    'Carrier market share by TEU and call volume per port per year. '
    'Primary view for Carrier Intelligence dashboard page.';

-- ─────────────────────────────────────────────────────────────
-- mart_port_comparison_annual
-- Grain: one row per port × year — executive summary view
--
-- FAN-OUT FIX: same two-CTE pattern as mart_port_monthly_throughput.
-- Previously, efficiency KPIs were computed from a query that
-- joined fact_port_calls to fact_cargo_movements before AVG(),
-- inflating every avg_* column by the per-call cargo-row count.
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW mart_port_comparison_annual AS

WITH calls AS (
    -- ── STEP 1: call-level KPIs at port × year grain ─────
    SELECT
        fpc.port_key,
        fpc.year,
        COUNT(*)                                            AS total_calls,
        COUNT(*) FILTER (WHERE fpc.is_delayed = 1)         AS delayed_calls,
        ROUND(AVG(fpc.berth_waiting_hours)::NUMERIC, 2)    AS avg_waiting_hours,
        ROUND(AVG(fpc.turnaround_hours)::NUMERIC,    2)    AS avg_turnaround_hours,
        ROUND(AVG(fpc.crane_moves_total)::NUMERIC,   0)    AS avg_crane_moves
    FROM fact_port_calls fpc
    GROUP BY fpc.port_key, fpc.year
),
cargo AS (
    -- ── STEP 2: cargo totals independently ───────────────
    SELECT
        fcm.port_key,
        EXTRACT(YEAR FROM dd.full_date)::INT                AS year,
        COALESCE(SUM(fcm.teu_count),       0)               AS total_teu,
        COALESCE(SUM(fcm.weight_tonnes),   0)               AS total_weight_tonnes,
        ROUND(COALESCE(SUM(fcm.cargo_value_usd), 0)::NUMERIC, 0)
                                                            AS total_cargo_value_usd
    FROM fact_cargo_movements fcm
    JOIN dim_date             dd ON dd.date_key = fcm.date_key
    GROUP BY fcm.port_key,
             EXTRACT(YEAR FROM dd.full_date)
)
-- ── STEP 3: join the two aggregations to dim_port ─────────────
SELECT
    dp.port_name,
    dp.country_name,
    dp.continent,
    dp.port_type,
    c.year,

    c.total_calls,
    COALESCE(g.total_teu,             0)                    AS total_teu,
    COALESCE(g.total_weight_tonnes,   0)                    AS total_weight_tonnes,
    COALESCE(g.total_cargo_value_usd, 0)                    AS total_cargo_value_usd,

    -- Efficiency KPIs: correctly averaged at call grain (no fan-out)
    c.avg_waiting_hours,
    c.avg_turnaround_hours,
    c.avg_crane_moves,

    ROUND(c.delayed_calls::NUMERIC / NULLIF(c.total_calls, 0) * 100, 1)
                                                            AS delay_rate_pct,

    -- Capacity utilisation: actual TEU vs declared annual capacity
    ROUND(
        COALESCE(g.total_teu, 0)::NUMERIC
        / NULLIF(dp.annual_capacity_teu, 0) * 100, 1
    )                                                       AS capacity_utilisation_pct

FROM calls        c
JOIN dim_port     dp ON dp.port_key = c.port_key
LEFT JOIN cargo   g  ON g.port_key  = c.port_key
                     AND g.year     = c.year

ORDER BY c.year, total_teu DESC;

COMMENT ON VIEW mart_port_comparison_annual IS
    'Annual cross-port executive scorecard. '
    'Fan-out fixed (v2): call KPIs and cargo totals aggregated in '
    'separate CTEs. First view to show in any portfolio demo.';
