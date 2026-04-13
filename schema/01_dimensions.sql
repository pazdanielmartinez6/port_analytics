-- =============================================================
-- PORT OPERATIONS ANALYTICS WAREHOUSE
-- 01_dimensions.sql  —  All dimension tables
-- Star schema grain:
--   fact_port_calls      → one row per vessel visit
--   fact_cargo_movements → one row per cargo batch per visit
-- =============================================================

SET client_min_messages = WARNING;

-- ─────────────────────────────────────────────────────────────
-- DIM_DATE
-- Date spine: 2022-01-01 → 2024-12-31
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_date (
    date_key          INTEGER      PRIMARY KEY,
    full_date         DATE         NOT NULL,
    year              SMALLINT     NOT NULL,
    quarter           SMALLINT     NOT NULL,
    month             SMALLINT     NOT NULL,
    month_name        VARCHAR(12)  NOT NULL,
    week_of_year      SMALLINT     NOT NULL,
    day_of_month      SMALLINT     NOT NULL,
    day_of_week       SMALLINT     NOT NULL,
    day_name          VARCHAR(12)  NOT NULL,
    is_weekend        SMALLINT     NOT NULL DEFAULT 0,
    fiscal_year       SMALLINT     NOT NULL,
    fiscal_quarter    SMALLINT     NOT NULL,
    -- Added in v2: holiday and season data populated by generate_data.py
    is_public_holiday SMALLINT     NOT NULL DEFAULT 0,
    holiday_name      VARCHAR(255) NOT NULL DEFAULT '',
    season_name       VARCHAR(10)  NOT NULL DEFAULT ''
);

COMMENT ON TABLE  dim_date IS 'Conformed date dimension covering the full 3-year analysis window.';
COMMENT ON COLUMN dim_date.date_key IS 'Surrogate key in YYYYMMDD integer format for fast range scans.';

-- ─────────────────────────────────────────────────────────────
-- DIM_PORT
-- Three study ports: Baltimore · Valencia · Naples
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_port (
    port_key              SERIAL       PRIMARY KEY,
    port_code             VARCHAR(10)  NOT NULL UNIQUE,
    port_name             VARCHAR(100) NOT NULL,
    country_code          CHAR(2)      NOT NULL,
    country_name          VARCHAR(100) NOT NULL,
    region                VARCHAR(50)  NOT NULL,
    continent             VARCHAR(50)  NOT NULL,
    un_locode             VARCHAR(10)  NOT NULL,
    latitude              NUMERIC(9,6) NOT NULL,
    longitude             NUMERIC(9,6) NOT NULL,
    port_type             VARCHAR(50)  NOT NULL,
    annual_capacity_teu   INTEGER      NOT NULL
);

COMMENT ON TABLE dim_port IS 'One row per study port. Calibrated to real geographic and capacity data.';

-- ─────────────────────────────────────────────────────────────
-- DIM_TERMINAL
-- 3 terminals per port (9 total)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_terminal (
    terminal_key          SERIAL       PRIMARY KEY,
    port_key              INTEGER      NOT NULL REFERENCES dim_port(port_key),
    terminal_code         VARCHAR(20)  NOT NULL UNIQUE,
    terminal_name         VARCHAR(100) NOT NULL,
    terminal_type         VARCHAR(30)  NOT NULL,   -- Container | RoRo | Multi-purpose | Bulk
    berths                SMALLINT     NOT NULL,
    max_draft_m           NUMERIC(5,2) NOT NULL,
    crane_count           SMALLINT     NOT NULL DEFAULT 0,
    annual_capacity_teu   INTEGER      NOT NULL DEFAULT 0
);

COMMENT ON TABLE dim_terminal IS 'Terminals within each port. Drives berth-level efficiency analysis.';

-- ─────────────────────────────────────────────────────────────
-- DIM_SHIP_SIZE_CLASS
-- UNCTAD / IMO standard size banding
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_ship_size_class (
    ship_size_class_key   SERIAL       PRIMARY KEY,
    class_name            VARCHAR(30)  NOT NULL UNIQUE,
    min_teu               INTEGER      NOT NULL,
    max_teu               INTEGER      NOT NULL,
    min_dwt               INTEGER      NOT NULL,
    max_dwt               INTEGER      NOT NULL,
    description           VARCHAR(255)
);

COMMENT ON TABLE dim_ship_size_class IS 'Vessel size bands aligned to UNCTAD classification for fleet benchmarking.';

-- ─────────────────────────────────────────────────────────────
-- DIM_VESSEL
-- 200 synthetic vessels with realistic attributes
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_vessel (
    vessel_key            SERIAL       PRIMARY KEY,
    imo_number            INTEGER      NOT NULL UNIQUE,
    vessel_name           VARCHAR(100) NOT NULL,
    vessel_type           VARCHAR(50)  NOT NULL,   -- Container Ship | Bulk Carrier | RoRo | …
    flag_country          VARCHAR(50)  NOT NULL,
    built_year            SMALLINT     NOT NULL,
    gross_tonnage         INTEGER      NOT NULL,
    deadweight_tonnes     INTEGER      NOT NULL,
    teu_capacity          INTEGER      NOT NULL DEFAULT 0,
    loa_meters            SMALLINT     NOT NULL,
    ship_size_class_key   INTEGER      NOT NULL REFERENCES dim_ship_size_class(ship_size_class_key)
);

COMMENT ON TABLE dim_vessel IS '200 synthetic vessels; IMO numbers are fictional but structurally valid.';

-- ─────────────────────────────────────────────────────────────
-- DIM_CARRIER
-- 15 real ocean carriers with alliance membership
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_carrier (
    carrier_key     SERIAL       PRIMARY KEY,
    carrier_code    VARCHAR(10)  NOT NULL UNIQUE,
    carrier_name    VARCHAR(100) NOT NULL,
    alliance        VARCHAR(50)  NOT NULL,
    hq_country      VARCHAR(50)  NOT NULL
);

COMMENT ON TABLE dim_carrier IS 'Ocean carrier dimension including alliance grouping for market-share analysis.';

-- ─────────────────────────────────────────────────────────────
-- DIM_CARGO_TYPE
-- 8 cargo categories covering container, bulk, RoRo
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_cargo_type (
    cargo_type_key    SERIAL       PRIMARY KEY,
    cargo_type_code   VARCHAR(10)  NOT NULL UNIQUE,
    cargo_type_name   VARCHAR(60)  NOT NULL,
    cargo_category    VARCHAR(30)  NOT NULL,   -- Container | Bulk | RoRo | General
    unit              VARCHAR(20)  NOT NULL,   -- TEU | Tonnes | Units
    is_hazmat_capable BOOLEAN      NOT NULL DEFAULT FALSE
);

COMMENT ON TABLE dim_cargo_type IS 'Cargo classification used to split throughput by commodity family.';

-- ─────────────────────────────────────────────────────────────
-- DIM_DIRECTION
-- Import · Export · Transshipment · Coastal
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_direction (
    direction_key   SERIAL       PRIMARY KEY,
    direction_code  VARCHAR(5)   NOT NULL UNIQUE,
    direction_name  VARCHAR(30)  NOT NULL,
    description     VARCHAR(255)
);

-- ─────────────────────────────────────────────────────────────
-- DIM_COUNTRY
-- 30 major trading-partner countries
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_country (
    country_key     SERIAL       PRIMARY KEY,
    country_code    CHAR(2)      NOT NULL UNIQUE,
    country_name    VARCHAR(100) NOT NULL,
    region          VARCHAR(50)  NOT NULL,
    continent       VARCHAR(50)  NOT NULL
);

-- ─────────────────────────────────────────────────────────────
-- DIM_ROUTE  (optional dimension — included)
-- 15 named trade lanes
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_route (
    route_key            SERIAL       PRIMARY KEY,
    route_code           VARCHAR(10)  NOT NULL UNIQUE,
    route_name           VARCHAR(100) NOT NULL,
    trade_lane           VARCHAR(60)  NOT NULL,
    origin_region        VARCHAR(50)  NOT NULL,
    destination_region   VARCHAR(50)  NOT NULL,
    avg_transit_days     SMALLINT     NOT NULL
);

COMMENT ON TABLE dim_route IS 'Named trade lanes enabling congestion and seasonality analysis by corridor.';

-- ─────────────────────────────────────────────────────────────
-- DIM_INCOTERM  (optional dimension — included)
-- All 11 Incoterms 2020 rules
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_incoterm (
    incoterm_key              SERIAL       PRIMARY KEY,
    incoterm_code             VARCHAR(5)   NOT NULL UNIQUE,
    incoterm_name             VARCHAR(60)  NOT NULL,
    risk_transfer             VARCHAR(60)  NOT NULL,
    freight_responsibility    VARCHAR(10)  NOT NULL,   -- Buyer | Seller
    insurance_responsibility  VARCHAR(10)  NOT NULL
);

COMMENT ON TABLE dim_incoterm IS 'Incoterms 2020 dimension for trade-finance and risk-transfer analysis.';
