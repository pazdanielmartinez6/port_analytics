-- =============================================================
-- models/staging/stg_cargo_movements.sql  (v2)
-- Purpose : Staging layer for fact_cargo_movements.
--           Light cleansing and enrichment — no aggregation.
--           Grain: one row per cargo batch (mirrors source fact).
--
-- Enhancements vs v1
-- ─────────────────────────────────────────────────────────────
-- • COALESCE null-safe guards on all numeric measures
-- • is_weekend flag joined from dim_date (authoritative)
-- • is_public_holiday and season_name surfaced from dim_date
-- • turnaround_band pulled from parent port call via FK join
--   (allows cargo-level turnaround slicing without a separate
--    join in mart queries)
-- • port_region pulled from dim_port for regional aggregations
-- • cargo_family simplified label (preserves existing logic)
-- • cargo_value_tier enhanced to include currency symbol
--
-- Naming/comment conventions match the existing mart views.
-- =============================================================

CREATE OR REPLACE VIEW stg_cargo_movements AS

SELECT
    -- ── Surrogate and foreign keys (pass-through) ─────────
    fcm.cargo_movement_id,
    fcm.port_call_id,
    fcm.date_key,
    fcm.port_key,
    fcm.terminal_key,
    fcm.vessel_key,
    fcm.carrier_key,
    fcm.cargo_type_key,
    fcm.direction_key,
    fcm.country_key,
    fcm.route_key,
    fcm.incoterm_key,
    fcm.ship_size_class_key,

    -- ── Numeric measures: null-safe COALESCE ──────────────
    COALESCE(fcm.teu_count,        0)                       AS teu_count,
    COALESCE(fcm.weight_tonnes,    0)                       AS weight_tonnes,
    COALESCE(fcm.unit_count,       0)                       AS unit_count,
    COALESCE(fcm.cargo_value_usd,  0)                       AS cargo_value_usd,
    COALESCE(fcm.is_hazardous,     0)                       AS is_hazardous,

    -- ── Derived: weight per TEU ────────────────────────────
    -- Data-quality check: typical range 8–25 tonnes per TEU.
    -- NULL when teu_count = 0 (bulk/RoRo cargo).
    CASE
        WHEN COALESCE(fcm.teu_count, 0) > 0
        THEN ROUND(
            COALESCE(fcm.weight_tonnes, 0)::NUMERIC
            / fcm.teu_count, 2)
        ELSE NULL
    END                                                     AS tonnes_per_teu,

    -- ── Derived: cargo value tier (revenue segmentation) ──
    CASE
        WHEN COALESCE(fcm.cargo_value_usd, 0) >= 10_000_000 THEN 'High Value (≥$10M)'
        WHEN COALESCE(fcm.cargo_value_usd, 0) >=  1_000_000 THEN 'Medium Value ($1M–$10M)'
        WHEN COALESCE(fcm.cargo_value_usd, 0) >=    100_000 THEN 'Standard ($100k–$1M)'
        ELSE                                                      'Low Value (<$100k)'
    END                                                     AS cargo_value_tier,

    -- ── Derived: simplified cargo family ──────────────────
    -- Maps cargo_type_key to a five-bucket family for high-level
    -- dashboard comparisons (preserves v1 mapping).
    CASE fcm.cargo_type_key
        WHEN 1 THEN 'Container'
        WHEN 2 THEN 'Container'
        WHEN 3 THEN 'Reefer'
        WHEN 4 THEN 'Dry Bulk'
        WHEN 5 THEN 'Liquid Bulk'
        WHEN 6 THEN 'RoRo'
        WHEN 7 THEN 'Break Bulk'
        WHEN 8 THEN 'Hazardous'
        ELSE        'Other'
    END                                                     AS cargo_family,

    -- ── Derived: weekend flag from dim_date (authoritative) ──
    -- Authoritative source: dim_date.is_weekend, not EXTRACT(DOW).
    -- Ensures consistency with stg_port_calls and mart views.
    COALESCE(dd.is_weekend, 0)                              AS is_weekend,

    -- ── Derived: public holiday flag ──────────────────────
    -- Enables analysis of holiday throughput vs. normal days.
    COALESCE(dd.is_public_holiday, 0)                       AS is_public_holiday,

    -- ── Derived: season name ──────────────────────────────
    -- Winter / Spring / Summer / Autumn (Northern hemisphere).
    COALESCE(dd.season_name, '')                            AS season_name,

    -- ── Derived: turnaround band from parent port call ────
    -- Avoids re-joining fact_port_calls in mart queries when
    -- cargo movements need to be sliced by call performance.
    CASE
        WHEN COALESCE(fpc.turnaround_hours, 0) < 24 THEN 'Fast (<24h)'
        WHEN COALESCE(fpc.turnaround_hours, 0) < 48 THEN 'Normal (24-48h)'
        WHEN COALESCE(fpc.turnaround_hours, 0) < 72 THEN 'Slow (48-72h)'
        ELSE                                              'Very Slow (>72h)'
    END                                                     AS turnaround_band,

    -- ── Derived: port region (from dim_port) ──────────────
    -- Enables regional aggregation in mart models without an
    -- extra join to dim_port.
    dp.region                                               AS port_region

FROM fact_cargo_movements fcm
-- Join dim_date for calendar flags
JOIN dim_date         dd  ON dd.date_key     = fcm.date_key
-- Join dim_port for region context
JOIN dim_port         dp  ON dp.port_key     = fcm.port_key
-- Join fact_port_calls for parent call turnaround band
JOIN fact_port_calls  fpc ON fpc.port_call_id = fcm.port_call_id;

COMMENT ON VIEW stg_cargo_movements IS
    'Staging view for fact_cargo_movements (v2). '
    'Null-safe numeric measures; cargo_family and cargo_value_tier labels; '
    'tonnes_per_teu quality check; is_weekend + is_public_holiday + '
    'season_name from dim_date; turnaround_band from parent port call; '
    'port_region from dim_port. Grain unchanged: one row per cargo batch.';
