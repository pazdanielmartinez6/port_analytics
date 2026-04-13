-- =============================================================
-- models/marts/mart_vessel_performance.sql
-- Purpose : Vessel and carrier performance mart.
--           Aggregates dwell time, delay rate, and throughput
--           per carrier × port × ship size class × year.
-- =============================================================

CREATE OR REPLACE VIEW mart_vessel_performance AS

WITH call_metrics AS (
    SELECT
        fpc.carrier_key,
        fpc.ship_size_class_key,
        fpc.port_key,
        fpc.year,
        COUNT(*)                                       AS total_calls,
        SUM(fpc.is_delayed)                            AS delayed_calls,
        ROUND(AVG(fpc.berth_waiting_hours)::NUMERIC,2) AS avg_waiting_hours,
        ROUND(AVG(fpc.at_berth_hours)::NUMERIC,     2) AS avg_berth_hours,
        ROUND(AVG(fpc.turnaround_hours)::NUMERIC,   2) AS avg_turnaround_hours,
        ROUND(AVG(fpc.delay_hours)::NUMERIC,         2) AS avg_delay_hours,
        ROUND(AVG(fpc.crane_moves_total)::NUMERIC,  0) AS avg_crane_moves
    FROM fact_port_calls fpc
    GROUP BY fpc.carrier_key, fpc.ship_size_class_key,
             fpc.port_key, fpc.year
),
cargo_metrics AS (
    SELECT
        fcm.carrier_key,
        fcm.ship_size_class_key,
        fcm.port_key,
        dd.year,
        SUM(fcm.teu_count)      AS total_teu,
        SUM(fcm.weight_tonnes)  AS total_weight_tonnes
    FROM fact_cargo_movements fcm
    JOIN dim_date dd ON dd.date_key = fcm.date_key
    GROUP BY fcm.carrier_key, fcm.ship_size_class_key,
             fcm.port_key, dd.year
)
SELECT
    dc.carrier_name,
    dc.alliance,
    dc.hq_country,
    dss.class_name          AS ship_size_class,
    dp.port_name,
    cm.year,

    cm.total_calls,
    cm.delayed_calls,
    ROUND(cm.delayed_calls::NUMERIC /
          NULLIF(cm.total_calls, 0) * 100, 1)          AS delay_rate_pct,

    cm.avg_waiting_hours,
    cm.avg_berth_hours,
    cm.avg_turnaround_hours,
    cm.avg_delay_hours,
    cm.avg_crane_moves,

    COALESCE(g.total_teu,           0) AS total_teu,
    COALESCE(g.total_weight_tonnes, 0) AS total_weight_tonnes,

    -- TEU per call (vessel utilisation proxy)
    ROUND(
        COALESCE(g.total_teu, 0)::NUMERIC
        / NULLIF(cm.total_calls, 0), 0
    )                                                   AS avg_teu_per_call

FROM call_metrics cm
JOIN dim_carrier         dc  ON dc.carrier_key          = cm.carrier_key
JOIN dim_ship_size_class dss ON dss.ship_size_class_key = cm.ship_size_class_key
JOIN dim_port            dp  ON dp.port_key             = cm.port_key
LEFT JOIN cargo_metrics  g   ON g.carrier_key           = cm.carrier_key
                             AND g.ship_size_class_key  = cm.ship_size_class_key
                             AND g.port_key             = cm.port_key
                             AND g.year                 = cm.year

ORDER BY cm.year, dp.port_name, cm.total_teu DESC;

COMMENT ON VIEW mart_vessel_performance IS
    'Carrier × ship-size performance mart. '
    'Answers: which carriers turn vessels fastest? '
    'Which size classes drive the most throughput?';


-- =============================================================
-- models/marts/mart_route_analysis.sql
-- Purpose : Route-level congestion and cargo-flow analysis.
-- =============================================================

CREATE OR REPLACE VIEW mart_route_analysis AS

WITH route_base AS (
    SELECT
        fcm.route_key,
        fcm.port_key,
        fcm.direction_key,
        dd.year,
        dd.quarter,
        COUNT(*)                      AS movement_count,
        SUM(fcm.teu_count)            AS total_teu,
        SUM(fcm.weight_tonnes)        AS total_weight_tonnes,
        ROUND(SUM(fcm.cargo_value_usd)::NUMERIC, 0) AS total_value_usd
    FROM fact_cargo_movements fcm
    JOIN dim_date dd ON dd.date_key = fcm.date_key
    GROUP BY fcm.route_key, fcm.port_key, fcm.direction_key,
             dd.year, dd.quarter
),
route_delays AS (
    -- Average delay for calls on each route
    SELECT
        fpc.route_key,
        fpc.port_key,
        fpc.year,
        fpc.month / 3 + 1       AS quarter,
        ROUND(AVG(fpc.berth_waiting_hours)::NUMERIC, 2) AS avg_waiting_hours,
        ROUND(AVG(fpc.turnaround_hours)::NUMERIC,    2) AS avg_turnaround_hours,
        ROUND(SUM(fpc.is_delayed)::NUMERIC /
              COUNT(*) * 100, 1)                         AS delay_rate_pct
    FROM fact_port_calls fpc
    GROUP BY fpc.route_key, fpc.port_key, fpc.year, fpc.month / 3 + 1
)
SELECT
    dr.route_name,
    dr.trade_lane,
    dr.origin_region,
    dr.destination_region,
    dr.avg_transit_days,
    dp.port_name,
    ddir.direction_name,
    rb.year,
    rb.quarter,

    rb.movement_count,
    rb.total_teu,
    rb.total_weight_tonnes,
    rb.total_value_usd,

    COALESCE(rd.avg_waiting_hours,   0) AS avg_waiting_hours,
    COALESCE(rd.avg_turnaround_hours,0) AS avg_turnaround_hours,
    COALESCE(rd.delay_rate_pct,      0) AS delay_rate_pct,

    -- Congestion index: normalise waiting time by route average transit
    ROUND(
        COALESCE(rd.avg_waiting_hours, 0) /
        NULLIF(dr.avg_transit_days * 24, 0) * 100, 2
    )                                               AS congestion_index_pct

FROM route_base             rb
JOIN dim_route              dr   ON dr.route_key     = rb.route_key
JOIN dim_port               dp   ON dp.port_key      = rb.port_key
JOIN dim_direction          ddir ON ddir.direction_key = rb.direction_key
LEFT JOIN route_delays      rd   ON rd.route_key     = rb.route_key
                                AND rd.port_key      = rb.port_key
                                AND rd.year          = rb.year
                                AND rd.quarter       = rb.quarter

ORDER BY rb.year, rb.quarter, dp.port_name, rb.total_teu DESC;

COMMENT ON VIEW mart_route_analysis IS
    'Route-level cargo flow and congestion analysis. '
    'Congestion index = port waiting time as % of average route transit time.';
