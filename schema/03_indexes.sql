-- =============================================================
-- PORT OPERATIONS ANALYTICS WAREHOUSE
-- 03_indexes.sql  —  Performance indexes for analytical queries
-- =============================================================

SET client_min_messages = WARNING;

-- ─────────────────────────────────────────────────────────────
-- fact_port_calls indexes
-- ─────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_fpc_date_key         ON fact_port_calls (date_key);
CREATE INDEX IF NOT EXISTS idx_fpc_port_key         ON fact_port_calls (port_key);
CREATE INDEX IF NOT EXISTS idx_fpc_terminal_key     ON fact_port_calls (terminal_key);
CREATE INDEX IF NOT EXISTS idx_fpc_vessel_key       ON fact_port_calls (vessel_key);
CREATE INDEX IF NOT EXISTS idx_fpc_carrier_key      ON fact_port_calls (carrier_key);
CREATE INDEX IF NOT EXISTS idx_fpc_route_key        ON fact_port_calls (route_key);
CREATE INDEX IF NOT EXISTS idx_fpc_year_month       ON fact_port_calls (year, month);
CREATE INDEX IF NOT EXISTS idx_fpc_arrival          ON fact_port_calls (arrival_datetime);
CREATE INDEX IF NOT EXISTS idx_fpc_is_delayed       ON fact_port_calls (is_delayed) WHERE is_delayed = 1;

-- ─────────────────────────────────────────────────────────────
-- fact_cargo_movements indexes
-- ─────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_fcm_port_call_id     ON fact_cargo_movements (port_call_id);
CREATE INDEX IF NOT EXISTS idx_fcm_date_key         ON fact_cargo_movements (date_key);
CREATE INDEX IF NOT EXISTS idx_fcm_port_key         ON fact_cargo_movements (port_key);
CREATE INDEX IF NOT EXISTS idx_fcm_terminal_key     ON fact_cargo_movements (terminal_key);
CREATE INDEX IF NOT EXISTS idx_fcm_carrier_key      ON fact_cargo_movements (carrier_key);
CREATE INDEX IF NOT EXISTS idx_fcm_cargo_type_key   ON fact_cargo_movements (cargo_type_key);
CREATE INDEX IF NOT EXISTS idx_fcm_direction_key    ON fact_cargo_movements (direction_key);
CREATE INDEX IF NOT EXISTS idx_fcm_country_key      ON fact_cargo_movements (country_key);
CREATE INDEX IF NOT EXISTS idx_fcm_route_key        ON fact_cargo_movements (route_key);
CREATE INDEX IF NOT EXISTS idx_fcm_hazardous        ON fact_cargo_movements (is_hazardous) WHERE is_hazardous = 1;

-- Composite: most common join pattern in throughput queries
CREATE INDEX IF NOT EXISTS idx_fcm_port_date        ON fact_cargo_movements (port_key, date_key);
CREATE INDEX IF NOT EXISTS idx_fcm_port_cargo       ON fact_cargo_movements (port_key, cargo_type_key);
