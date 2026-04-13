-- =============================================================
-- PORT OPERATIONS ANALYTICS WAREHOUSE
-- 05_operations.sql  —  Terminal Operating System (TOS) Layer
--
-- Purpose : Adds an OLTP-style operational layer that sits
--           alongside the existing analytical star schema.
--           All tables write-back to ops_* tables via the
--           FastAPI service (api/main.py).
--
-- Design principles:
--   • All tables have created_at TIMESTAMPTZ for audit trail.
--   • FKs reference existing dim_* tables where applicable.
--   • All DDL is idempotent (CREATE TABLE IF NOT EXISTS,
--     CREATE TYPE IF NOT EXISTS, ALTER TABLE … ADD COLUMN IF NOT EXISTS).
--   • Inline COMMENT ON every table and every column.
--
-- Run order: after 01_dimensions.sql and 02_facts.sql.
-- =============================================================

SET client_min_messages = WARNING;

-- ─────────────────────────────────────────────────────────────
-- SECTION A: Enhance dim_date with holiday + season columns
-- These columns are populated by generate_data.py (v2).
-- ─────────────────────────────────────────────────────────────

-- is_public_holiday: 1 if the date is a national public holiday
-- in any of the three study port countries (US / ES / IT).
ALTER TABLE dim_date
    ADD COLUMN IF NOT EXISTS is_public_holiday SMALLINT NOT NULL DEFAULT 0;

-- holiday_name: pipe-separated list of holiday names across countries.
-- Empty string when is_public_holiday = 0.
ALTER TABLE dim_date
    ADD COLUMN IF NOT EXISTS holiday_name VARCHAR(255) NOT NULL DEFAULT '';

-- season_name: meteorological season for Northern-hemisphere ports.
-- Values: Winter | Spring | Summer | Autumn
ALTER TABLE dim_date
    ADD COLUMN IF NOT EXISTS season_name VARCHAR(10) NOT NULL DEFAULT '';

COMMENT ON COLUMN dim_date.is_public_holiday IS
    '1 = public holiday in at least one of the three study-port countries (US/ES/IT).';
COMMENT ON COLUMN dim_date.holiday_name IS
    'Pipe-separated holiday names, e.g. "Christmas Day | Navidad | Natale". '
    'Empty when is_public_holiday = 0.';
COMMENT ON COLUMN dim_date.season_name IS
    'Meteorological season (Northern hemisphere): Winter / Spring / Summer / Autumn. '
    'Derived from calendar month.';

-- ─────────────────────────────────────────────────────────────
-- SECTION B: ENUM types for operational events
-- ─────────────────────────────────────────────────────────────

-- Container event type ENUM (idempotent via DO block)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_type WHERE typname = 'container_event_type'
    ) THEN
        CREATE TYPE container_event_type AS ENUM (
            'gate-in',           -- Truck delivers container at gate
            'discharge',         -- Container discharged from vessel to quay
            'load',              -- Container loaded from yard to vessel
            'shift',             -- Container moved within yard (re-stow)
            'gate-out',          -- Container departs gate to inland
            'maintenance-block', -- Container moved due to maintenance reservation
            'release'            -- Customs/health authority release event
        );
    END IF;
END$$;

COMMENT ON TYPE container_event_type IS
    'Lifecycle events tracked for each container movement in the TOS layer.';

-- Block type ENUM
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_type WHERE typname = 'yard_block_type'
    ) THEN
        CREATE TYPE yard_block_type AS ENUM (
            'import',   -- Import containers awaiting collection
            'export',   -- Export containers awaiting loading
            'reefer',   -- Temperature-controlled containers (powered)
            'hazmat',   -- Dangerous goods (IMDG-compliant segregation)
            'empty'     -- Empty containers awaiting reuse or export
        );
    END IF;
END$$;

COMMENT ON TYPE yard_block_type IS
    'Classification of a yard block by the cargo type it is designated to hold.';

-- ─────────────────────────────────────────────────────────────
-- SECTION C: OPS_YARD_BLOCK
-- One row per physical yard block in a terminal.
-- A block is a named rectangular grid area (row × bay × tier).
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ops_yard_block (
    yard_block_id   SERIAL          PRIMARY KEY,

    -- ── Dimensional links ─────────────────────────────────
    port_key        INTEGER         NOT NULL
                    REFERENCES dim_port(port_key)
                    ON UPDATE CASCADE,

    terminal_key    INTEGER         NOT NULL
                    REFERENCES dim_terminal(terminal_key)
                    ON UPDATE CASCADE,

    -- ── Block identity ────────────────────────────────────
    block_code      VARCHAR(10)     NOT NULL,
    -- Human-readable block identifier, e.g. "A01", "R03".
    -- Unique within a terminal.
    CONSTRAINT uq_yard_block_terminal_code UNIQUE (terminal_key, block_code),

    -- ── Physical dimensions ───────────────────────────────
    row_count       SMALLINT        NOT NULL CHECK (row_count > 0),
    -- Number of rows in the block (North–South axis).

    bay_count       SMALLINT        NOT NULL CHECK (bay_count > 0),
    -- Number of bays in the block (East–West axis).

    tier_count      SMALLINT        NOT NULL CHECK (tier_count > 0),
    -- Maximum stacking height in tiers (typical: 4–6 for standard, 1 for flat).

    block_type      yard_block_type NOT NULL,
    -- Category of containers this block is designated to hold.

    -- ── Operational flags ─────────────────────────────────
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    -- FALSE when a block is permanently closed or under major reconstruction.

    -- ── Audit ─────────────────────────────────────────────
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now()
);

COMMENT ON TABLE  ops_yard_block IS
    'TOS layer: one row per physical yard block within a terminal. '
    'Capacity = row_count × bay_count × tier_count containers. '
    'FKs link to dim_port and dim_terminal.';
COMMENT ON COLUMN ops_yard_block.block_code  IS 'Short mnemonic used by TOS (e.g. A01, R03). Unique per terminal.';
COMMENT ON COLUMN ops_yard_block.row_count   IS 'Number of rows (N–S) in the block.';
COMMENT ON COLUMN ops_yard_block.bay_count   IS 'Number of bays (E–W) in the block.';
COMMENT ON COLUMN ops_yard_block.tier_count  IS 'Maximum stacking height (tiers). Reefer blocks typically limited to 2.';
COMMENT ON COLUMN ops_yard_block.block_type  IS 'Cargo designation: import / export / reefer / hazmat / empty.';
COMMENT ON COLUMN ops_yard_block.is_active   IS 'FALSE if block is closed or permanently decommissioned.';
COMMENT ON COLUMN ops_yard_block.created_at  IS 'Row creation timestamp (UTC).';

-- ─────────────────────────────────────────────────────────────
-- SECTION D: OPS_CONTAINER
-- Master record for each individual container tracked in the TOS.
-- Position is stored denormalised for fast real-time queries.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ops_container (
    container_number    VARCHAR(11)     PRIMARY KEY,
    -- ISO 6346 container number: 4-char owner code + 7-digit serial + check digit.
    -- Example: MSCU1234567

    -- ── ISO type + physical attributes ───────────────────
    iso_type_code       CHAR(4)         NOT NULL,
    -- ISO 6346 type/size code (e.g. 22G1 = 20ft standard dry, 45R1 = 45ft reefer).

    size_feet           SMALLINT        NOT NULL CHECK (size_feet IN (20, 40, 45, 48)),
    -- Nominal container length in feet.

    -- ── Current lifecycle state ───────────────────────────
    status              VARCHAR(20)     NOT NULL
                        CHECK (status IN (
                            'pre-advised', 'on-terminal', 'in-yard',
                            'at-quay', 'loaded', 'departed', 'on-hold'
                        )),
    -- Lifecycle state aligned to TOS event model.

    -- ── Current physical position (nullable until placed in yard) ─
    current_block       VARCHAR(10)     NULL,
    -- Block code from ops_yard_block; NULL when not yet placed.

    current_row         SMALLINT        NULL CHECK (current_row > 0),
    current_bay         SMALLINT        NULL CHECK (current_bay > 0),
    current_tier        SMALLINT        NULL CHECK (current_tier > 0),

    -- ── Dimensional links ─────────────────────────────────
    port_key            INTEGER         NOT NULL
                        REFERENCES dim_port(port_key),

    terminal_key        INTEGER         NOT NULL
                        REFERENCES dim_terminal(terminal_key),

    vessel_key          INTEGER         NULL
                        REFERENCES dim_vessel(vessel_key),
    -- NULL when container is not associated with an active vessel call.

    carrier_key         INTEGER         NOT NULL
                        REFERENCES dim_carrier(carrier_key),

    -- ── Cargo attributes ─────────────────────────────────
    is_hazardous        BOOLEAN         NOT NULL DEFAULT FALSE,
    -- TRUE when container carries IMDG-classified dangerous goods.

    is_reefer           BOOLEAN         NOT NULL DEFAULT FALSE,
    -- TRUE when container requires active temperature control.

    cargo_value_usd     NUMERIC(15, 2)  NOT NULL DEFAULT 0
                        CHECK (cargo_value_usd >= 0),
    -- Declared cargo value in USD (used for customs and insurance).

    -- ── Audit ─────────────────────────────────────────────
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now()
);

COMMENT ON TABLE  ops_container IS
    'TOS layer: master record for each individual container on terminal. '
    'Position columns (current_block / row / bay / tier) are updated by '
    'every movement event via api/main.py. '
    'Use ops_container_event for full movement audit trail.';
COMMENT ON COLUMN ops_container.container_number IS 'ISO 6346 container identifier (11 chars). Natural primary key.';
COMMENT ON COLUMN ops_container.iso_type_code    IS 'ISO 6346 4-char type/size code, e.g. 22G1, 42G1, 45R1.';
COMMENT ON COLUMN ops_container.size_feet        IS 'Nominal container length: 20, 40, 45, or 48 feet.';
COMMENT ON COLUMN ops_container.status           IS 'Lifecycle state. Updated by TOS event processing.';
COMMENT ON COLUMN ops_container.current_block    IS 'Block code of current yard position. NULL if not in yard.';
COMMENT ON COLUMN ops_container.current_row      IS 'Current row within block.';
COMMENT ON COLUMN ops_container.current_bay      IS 'Current bay within block.';
COMMENT ON COLUMN ops_container.current_tier     IS 'Current tier within stack (1 = ground level).';
COMMENT ON COLUMN ops_container.vessel_key       IS 'FK to dim_vessel. NULL when not on a vessel.';
COMMENT ON COLUMN ops_container.is_hazardous     IS 'TRUE = IMDG dangerous goods. Drives hazmat block routing.';
COMMENT ON COLUMN ops_container.is_reefer        IS 'TRUE = temperature-controlled. Must be placed in reefer block.';
COMMENT ON COLUMN ops_container.cargo_value_usd  IS 'Declared USD cargo value for customs and insurance purposes.';
COMMENT ON COLUMN ops_container.created_at       IS 'Row creation timestamp (UTC).';

-- ─────────────────────────────────────────────────────────────
-- SECTION E: OPS_CONTAINER_EVENT
-- Full audit log of every container movement and status change.
-- Append-only: never update or delete rows in this table.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ops_container_event (
    event_id            BIGSERIAL       PRIMARY KEY,

    -- ── Links ─────────────────────────────────────────────
    container_number    VARCHAR(11)     NOT NULL
                        REFERENCES ops_container(container_number)
                        ON UPDATE CASCADE,

    port_call_id        INTEGER         NULL
                        REFERENCES fact_port_calls(port_call_id)
                        ON DELETE SET NULL,
    -- Optional FK to the vessel call that triggered the event.
    -- NULL for gate-in, gate-out, and yard maintenance moves.

    -- ── Event details ─────────────────────────────────────
    event_type          container_event_type  NOT NULL,

    from_position       VARCHAR(50)     NULL,
    -- Encoded as "BLOCK:ROW:BAY:TIER" (e.g. "A01:3:12:2").
    -- NULL for gate-in (no prior position).

    to_position         VARCHAR(50)     NULL,
    -- Encoded as "BLOCK:ROW:BAY:TIER".
    -- NULL for gate-out or release (no subsequent yard position).

    operator            VARCHAR(100)    NOT NULL,
    -- Name or ID of the TOS operator who recorded the event.

    event_timestamp     TIMESTAMPTZ     NOT NULL DEFAULT now(),
    -- When the physical movement occurred (UTC).

    -- ── Audit ─────────────────────────────────────────────
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now()
    -- When this event row was inserted (may differ from event_timestamp
    -- if recorded retrospectively).
);

COMMENT ON TABLE  ops_container_event IS
    'TOS layer: append-only event log for all container movements. '
    'Never UPDATE or DELETE rows — this is the authoritative audit trail. '
    'Positions encoded as BLOCK:ROW:BAY:TIER strings.';
COMMENT ON COLUMN ops_container_event.event_id          IS 'Auto-incrementing surrogate key. Use for pagination.';
COMMENT ON COLUMN ops_container_event.container_number  IS 'FK to ops_container. Indexed for fast container history queries.';
COMMENT ON COLUMN ops_container_event.port_call_id      IS 'FK to fact_port_calls (nullable). Links event to a vessel call.';
COMMENT ON COLUMN ops_container_event.event_type        IS 'Event type ENUM: gate-in / discharge / load / shift / gate-out / maintenance-block / release.';
COMMENT ON COLUMN ops_container_event.from_position     IS 'Previous position encoded as BLOCK:ROW:BAY:TIER. NULL for gate-in.';
COMMENT ON COLUMN ops_container_event.to_position       IS 'New position encoded as BLOCK:ROW:BAY:TIER. NULL for gate-out.';
COMMENT ON COLUMN ops_container_event.operator          IS 'TOS operator name or system identifier that recorded the event.';
COMMENT ON COLUMN ops_container_event.event_timestamp   IS 'Physical movement timestamp (UTC). Set by caller, not DEFAULT.';
COMMENT ON COLUMN ops_container_event.created_at        IS 'Database insertion timestamp (UTC). Always DEFAULT now().';

-- ─────────────────────────────────────────────────────────────
-- SECTION F: OPS_MAINTENANCE_BLOCK
-- Scheduled and ad-hoc maintenance reservations on yard areas.
-- Prevents container placement in affected blocks during the window.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ops_maintenance_block (
    maintenance_id  SERIAL          PRIMARY KEY,

    -- ── Affected area ─────────────────────────────────────
    block_code      VARCHAR(10)     NOT NULL,
    -- Block code as stored in ops_yard_block.block_code.
    -- NOTE: no FK constraint to allow maintenance reservations
    -- on blocks not yet created in ops_yard_block (pre-planning).

    terminal_key    INTEGER         NOT NULL
                    REFERENCES dim_terminal(terminal_key),
    -- Disambiguates block_code across terminals.

    -- ── Time window ───────────────────────────────────────
    start_time      TIMESTAMPTZ     NOT NULL,
    end_time        TIMESTAMPTZ     NOT NULL,
    CONSTRAINT chk_maint_end_after_start CHECK (end_time > start_time),

    -- ── Reason and operator ───────────────────────────────
    reason          VARCHAR(255)    NOT NULL,
    -- Free-text description of the maintenance activity.
    -- e.g. "Reefer plug inspection", "Pavement resurfacing Bay 12-18".

    operator        VARCHAR(100)    NOT NULL,
    -- Name or ID of the person who created the reservation.

    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    -- FALSE when maintenance is cancelled or completed ahead of schedule.

    -- ── Audit ─────────────────────────────────────────────
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now()
);

COMMENT ON TABLE  ops_maintenance_block IS
    'TOS layer: maintenance reservations on yard block areas. '
    'Active reservations prevent new container placement in the affected block. '
    'TOS should check this table before any assign-slot operation.';
COMMENT ON COLUMN ops_maintenance_block.block_code    IS 'Yard block code (matches ops_yard_block.block_code).';
COMMENT ON COLUMN ops_maintenance_block.terminal_key  IS 'FK to dim_terminal. Required to disambiguate block_code.';
COMMENT ON COLUMN ops_maintenance_block.start_time    IS 'Maintenance window start (UTC). Container moves blocked from this time.';
COMMENT ON COLUMN ops_maintenance_block.end_time      IS 'Maintenance window end (UTC). Block reopens after this time.';
COMMENT ON COLUMN ops_maintenance_block.reason        IS 'Free-text reason for reservation (e.g. pavement repair, reefer check).';
COMMENT ON COLUMN ops_maintenance_block.operator      IS 'TOS user who created the reservation.';
COMMENT ON COLUMN ops_maintenance_block.is_active     IS 'FALSE = reservation cancelled or completed early.';
COMMENT ON COLUMN ops_maintenance_block.created_at    IS 'Row creation timestamp (UTC).';

-- ─────────────────────────────────────────────────────────────
-- SECTION G: Indexes for OPS tables
-- ─────────────────────────────────────────────────────────────

-- ops_yard_block
CREATE INDEX IF NOT EXISTS idx_oyb_port_terminal
    ON ops_yard_block (port_key, terminal_key);
CREATE INDEX IF NOT EXISTS idx_oyb_block_type
    ON ops_yard_block (block_type);

-- ops_container
CREATE INDEX IF NOT EXISTS idx_oc_status
    ON ops_container (status);
CREATE INDEX IF NOT EXISTS idx_oc_port_terminal
    ON ops_container (port_key, terminal_key);
CREATE INDEX IF NOT EXISTS idx_oc_carrier
    ON ops_container (carrier_key);
CREATE INDEX IF NOT EXISTS idx_oc_vessel
    ON ops_container (vessel_key) WHERE vessel_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_oc_block
    ON ops_container (current_block) WHERE current_block IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_oc_hazardous
    ON ops_container (is_hazardous) WHERE is_hazardous = TRUE;
CREATE INDEX IF NOT EXISTS idx_oc_reefer
    ON ops_container (is_reefer) WHERE is_reefer = TRUE;

-- ops_container_event (high-volume append table — targeted indexes only)
CREATE INDEX IF NOT EXISTS idx_oce_container
    ON ops_container_event (container_number, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_oce_port_call
    ON ops_container_event (port_call_id) WHERE port_call_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_oce_event_type
    ON ops_container_event (event_type);
CREATE INDEX IF NOT EXISTS idx_oce_timestamp
    ON ops_container_event (event_timestamp DESC);

-- ops_maintenance_block (small table; focus on active time-range queries)
CREATE INDEX IF NOT EXISTS idx_omb_terminal_active
    ON ops_maintenance_block (terminal_key, is_active)
    WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_omb_block_time
    ON ops_maintenance_block (block_code, start_time, end_time);

