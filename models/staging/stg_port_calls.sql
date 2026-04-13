-- =============================================================
-- models/staging/stg_port_calls.sql  (v2)
-- Purpose : Staging layer for fact_port_calls.
--           Light cleansing and enrichment — no aggregation.
--           Grain: one row per vessel visit (mirrors source fact).
--
-- Enhancements vs v1
-- ─────────────────────────────────────────────────────────────
-- • COALESCE null-safe guards on all numeric measures
-- • is_weekend derived from dim_date (authoritative) rather than
--   EXTRACT(DOW FROM arrival_datetime) — ensures alignment with
--   the date dimension used by mart views
-- • turnaround_band exposed as a labelled string (not just _id)
-- • port_region pulled from dim_port for regional slicing
-- • season_name and is_public_holiday surfaced from dim_date
-- • crane_moves_per_hour computed with null-safe divisor
--
-- Naming/comment conventions match the existing mart views.
-- =============================================================

CREATE OR REPLACE VIEW stg_port_calls AS

SELECT
    -- ── Surrogate and foreign keys (pass-through) ─────────
    fpc.port_call_id,
    fpc.date_key,
    fpc.port_key,
    fpc.terminal_key,
    fpc.vessel_key,
    fpc.carrier_key,
    fpc.ship_size_class_key,
    fpc.route_key,

    -- ── Operational timestamps (pass-through) ─────────────
    fpc.arrival_datetime,
    fpc.berth_datetime,
    fpc.departure_datetime,

    -- ── Numeric measures: null-safe COALESCE ──────────────
    -- fact_port_calls has NOT NULL constraints, but COALESCE is
    -- applied defensively as a staging best practice and to
    -- handle hypothetical NULL rows from FULL OUTER JOINs.
    COALESCE(fpc.berth_waiting_hours, 0)                    AS berth_waiting_hours,
    COALESCE(fpc.at_berth_hours,      0)                    AS at_berth_hours,
    COALESCE(fpc.turnaround_hours,    0)                    AS turnaround_hours,
    COALESCE(fpc.crane_moves_total,   0)                    AS crane_moves_total,
    COALESCE(fpc.is_delayed,          0)                    AS is_delayed,
    COALESCE(fpc.delay_hours,         0)                    AS delay_hours,

    -- ── Denormalised convenience columns ──────────────────
    fpc.year,
    fpc.month,

    -- ── Derived: turnaround performance band (numeric ID) ─
    CASE
        WHEN COALESCE(fpc.turnaround_hours, 0) < 24 THEN 1
        WHEN COALESCE(fpc.turnaround_hours, 0) < 48 THEN 2
        WHEN COALESCE(fpc.turnaround_hours, 0) < 72 THEN 3
        ELSE                                              4
    END                                                     AS turnaround_band_id,

    -- ── Derived: turnaround performance band (label) ──────
    -- Human-readable label used in BI filters and tooltips.
    CASE
        WHEN COALESCE(fpc.turnaround_hours, 0) < 24 THEN 'Fast (<24h)'
        WHEN COALESCE(fpc.turnaround_hours, 0) < 48 THEN 'Normal (24-48h)'
        WHEN COALESCE(fpc.turnaround_hours, 0) < 72 THEN 'Slow (48-72h)'
        ELSE                                              'Very Slow (>72h)'
    END                                                     AS turnaround_band,

    -- ── Derived: crane productivity (moves per berth-hour) ─
    CASE
        WHEN COALESCE(fpc.at_berth_hours, 0) > 0
        THEN ROUND(
            COALESCE(fpc.crane_moves_total, 0)::NUMERIC
            / fpc.at_berth_hours, 2)
        ELSE NULL
    END                                                     AS crane_moves_per_hour,

    -- ── Derived: waiting time classification ──────────────
    CASE
        WHEN COALESCE(fpc.berth_waiting_hours, 0) = 0   THEN 'No Wait'
        WHEN COALESCE(fpc.berth_waiting_hours, 0) <= 6  THEN 'Short (<6h)'
        WHEN COALESCE(fpc.berth_waiting_hours, 0) <= 24 THEN 'Medium (6-24h)'
        ELSE                                                  'Long (>24h)'
    END                                                     AS waiting_band,

    -- ── Derived: weekend flag from dim_date (authoritative) ──
    -- Using dd.is_weekend rather than DOW from arrival_datetime
    -- ensures this flag matches the date dimension used in mart views.
    COALESCE(dd.is_weekend, 0)                              AS is_weekend,

    -- ── Derived: public holiday flag ──────────────────────
    -- Allows analysis of whether port call efficiency differs
    -- on holidays (reduced labour, customs delays, etc.).
    COALESCE(dd.is_public_holiday, 0)                       AS is_public_holiday,

    -- ── Derived: season name ──────────────────────────────
    -- From dim_date.season_name (Winter / Spring / Summer / Autumn).
    COALESCE(dd.season_name, '')                            AS season_name,

    -- ── Derived: port region (from dim_port) ──────────────
    -- Enables regional aggregation without joining dim_port in
    -- every downstream mart query.
    dp.region                                               AS port_region

FROM fact_port_calls fpc
-- Join dim_date to get authoritative calendar flags
JOIN dim_date dd ON dd.date_key = fpc.date_key
-- Join dim_port to pull region for contextual slicing
JOIN dim_port dp ON dp.port_key = fpc.port_key;

COMMENT ON VIEW stg_port_calls IS
    'Staging view for fact_port_calls (v2). '
    'Null-safe numeric measures; turnaround_band and waiting_band labels; '
    'is_weekend and is_public_holiday from dim_date (authoritative); '
    'season_name from dim_date; port_region from dim_port. '
    'Grain unchanged: one row per vessel visit.';
