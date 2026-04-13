-- =============================================================
-- PORT OPERATIONS ANALYTICS WAREHOUSE
-- 02_facts.sql  —  Both fact tables
--
-- GRAIN DEFINITIONS
-- ─────────────────────────────────────────────────────────────
-- fact_port_calls      : one row = one vessel visit to one terminal
--                        (arrival → berth → departure)
--
-- fact_cargo_movements : one row = one cargo batch moved during
--                        a port call, split by cargo type and
--                        direction; FK to fact_port_calls via
--                        port_call_id
-- =============================================================

SET client_min_messages = WARNING;

-- ─────────────────────────────────────────────────────────────
-- FACT_PORT_CALLS
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_port_calls (
    port_call_id          SERIAL        PRIMARY KEY,
    date_key              INTEGER       NOT NULL REFERENCES dim_date(date_key),
    port_key              INTEGER       NOT NULL REFERENCES dim_port(port_key),
    terminal_key          INTEGER       NOT NULL REFERENCES dim_terminal(terminal_key),
    vessel_key            INTEGER       NOT NULL REFERENCES dim_vessel(vessel_key),
    carrier_key           INTEGER       NOT NULL REFERENCES dim_carrier(carrier_key),
    ship_size_class_key   INTEGER       NOT NULL REFERENCES dim_ship_size_class(ship_size_class_key),
    route_key             INTEGER       NOT NULL REFERENCES dim_route(route_key),

    -- Operational timestamps
    arrival_datetime      TIMESTAMP     NOT NULL,
    berth_datetime        TIMESTAMP     NOT NULL,
    departure_datetime    TIMESTAMP     NOT NULL,

    -- Degenerate / additive measures
    berth_waiting_hours   NUMERIC(8,2)  NOT NULL DEFAULT 0,   -- arrival → berth
    at_berth_hours        NUMERIC(8,2)  NOT NULL DEFAULT 0,   -- berth → departure
    turnaround_hours      NUMERIC(8,2)  NOT NULL DEFAULT 0,   -- arrival → departure (incl. delays)
    crane_moves_total     INTEGER       NOT NULL DEFAULT 0,   -- proxy for productivity
    is_delayed            SMALLINT      NOT NULL DEFAULT 0,   -- 1 = vessel experienced delay
    delay_hours           NUMERIC(8,2)  NOT NULL DEFAULT 0,

    -- Denormalised convenience columns (avoids joins for simple filters)
    year                  SMALLINT      NOT NULL,
    month                 SMALLINT      NOT NULL,

    CONSTRAINT chk_berth_after_arrival    CHECK (berth_datetime     >= arrival_datetime),
    CONSTRAINT chk_departure_after_berth  CHECK (departure_datetime >= berth_datetime),
    CONSTRAINT chk_positive_waiting       CHECK (berth_waiting_hours >= 0),
    CONSTRAINT chk_positive_berth         CHECK (at_berth_hours      >= 0),
    CONSTRAINT chk_is_delayed             CHECK (is_delayed IN (0, 1))
);

COMMENT ON TABLE  fact_port_calls IS
    'Grain: one row per vessel visit to a terminal. '
    'Calibrated call volumes: Baltimore ~650/yr, Valencia ~1050/yr, Naples ~550/yr. '
    'Source: synthetic data modelled on public port statistics (2022-2024).';

COMMENT ON COLUMN fact_port_calls.port_call_id       IS 'Natural key and surrogate; also FK target for fact_cargo_movements.';
COMMENT ON COLUMN fact_port_calls.berth_waiting_hours IS 'Anchorage / roads waiting time before berth assignment.';
COMMENT ON COLUMN fact_port_calls.turnaround_hours    IS 'Total port stay including any delay hours.';
COMMENT ON COLUMN fact_port_calls.crane_moves_total   IS 'Total lifts performed during the call (container vessels only).';

-- ─────────────────────────────────────────────────────────────
-- FACT_CARGO_MOVEMENTS
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_cargo_movements (
    cargo_movement_id     SERIAL          PRIMARY KEY,
    port_call_id          INTEGER         NOT NULL REFERENCES fact_port_calls(port_call_id),

    -- Conformed dimension FKs (denormalised from parent call for query convenience)
    date_key              INTEGER         NOT NULL REFERENCES dim_date(date_key),
    port_key              INTEGER         NOT NULL REFERENCES dim_port(port_key),
    terminal_key          INTEGER         NOT NULL REFERENCES dim_terminal(terminal_key),
    vessel_key            INTEGER         NOT NULL REFERENCES dim_vessel(vessel_key),
    carrier_key           INTEGER         NOT NULL REFERENCES dim_carrier(carrier_key),
    cargo_type_key        INTEGER         NOT NULL REFERENCES dim_cargo_type(cargo_type_key),
    direction_key         INTEGER         NOT NULL REFERENCES dim_direction(direction_key),
    country_key           INTEGER         NOT NULL REFERENCES dim_country(country_key),
    route_key             INTEGER         NOT NULL REFERENCES dim_route(route_key),
    incoterm_key          INTEGER         NOT NULL REFERENCES dim_incoterm(incoterm_key),
    ship_size_class_key   INTEGER         NOT NULL REFERENCES dim_ship_size_class(ship_size_class_key),

    -- Additive measures
    teu_count             INTEGER         NOT NULL DEFAULT 0,   -- 0 for non-container cargo
    weight_tonnes         NUMERIC(12,2)   NOT NULL DEFAULT 0,
    unit_count            INTEGER         NOT NULL DEFAULT 0,   -- vehicles (RoRo), pieces (break-bulk)
    cargo_value_usd       NUMERIC(15,2)   NOT NULL DEFAULT 0,
    is_hazardous          SMALLINT        NOT NULL DEFAULT 0,

    CONSTRAINT chk_non_negative_teu     CHECK (teu_count    >= 0),
    CONSTRAINT chk_non_negative_weight  CHECK (weight_tonnes >= 0),
    CONSTRAINT chk_non_negative_value   CHECK (cargo_value_usd >= 0),
    CONSTRAINT chk_is_hazardous         CHECK (is_hazardous IN (0, 1))
);

COMMENT ON TABLE  fact_cargo_movements IS
    'Grain: one row per cargo batch (by type and direction) within a port call. '
    'Target volume ~100,000 rows across all ports and 3 years. '
    'All dimension FKs are denormalised for star-schema query performance.';

COMMENT ON COLUMN fact_cargo_movements.teu_count       IS '20-foot equivalent units; 0 for bulk and RoRo cargo.';
COMMENT ON COLUMN fact_cargo_movements.weight_tonnes   IS 'Gross cargo weight in metric tonnes.';
COMMENT ON COLUMN fact_cargo_movements.unit_count      IS 'Vehicle units for RoRo; piece count for break-bulk; mirrors teu_count for containers.';
COMMENT ON COLUMN fact_cargo_movements.cargo_value_usd IS 'Estimated cargo value in USD (synthetic; calibrated to commodity benchmarks).';
