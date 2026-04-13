# CHANGELOG

All notable changes to the Port Operations Analytics Warehouse are documented here.  
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [2.0.0] — 2026-04

### Added

#### `scripts/generate_data.py` — Full rewrite (v2)
- **Realistic monthly seasonality per port:**
  - Valencia peaks June–August (`seasonality` weights 0.100–0.105 in those months)
  - Baltimore peaks October–December (retail import season, weights 0.100–0.110)
  - Naples peaks April–September (Mediterranean trade season, weights 0.095–0.105)
- **Carrier concentration by route:**
  - `CARRIER_ROUTE_AFFINITY` dict applies weight multipliers at call generation time
  - MSC (key 1) and Maersk (key 2) boosted 3×–3.5× on Asia–Europe routes (4, 5)
  - Hapag-Lloyd (key 6) boosted 3× on Trans-Atlantic routes (2, 3, 10)
  - Grimaldi (key 10) dominant on Intra-Mediterranean (route 6) at Naples
- **Correlated delay patterns:**
  - `_BAL_DELAY_MULT` array: Baltimore delay rate peaks in Nov–Feb (1.30–1.40×)
  - `_NAP_DELAY_MULT` array: Naples delay rate peaks in Jun–Aug (1.35–1.40×, congestion)
  - `_VLC_DELAY_MULT` array: Valencia mild modulation (0.90–1.10×)
  - Berth-waiting hours scale proportionally with monthly delay rate multiplier
- **Cargo type mix calibrated per port:**
  - Baltimore: RoRo weight 0.28, FCL reduced to 0.30 (was 0.40)
  - Valencia: FCL dominant at 0.58 (was 0.55)
  - Naples: RoRo weight 0.24, reflecting Grimaldi home port status
- **Year-on-year TEU growth of 3–5%:**
  - `YOY_GROWTH = {2022: 1.000, 2023: 1.038, 2024: 1.078}` applied to call volumes and TEU counts
- **Public holidays and season columns** added to `gen_dim_date()`:
  - `_us_holidays(year)` — US federal holidays (Baltimore): New Year, MLK Day, Presidents Day, Memorial Day, Independence Day, Labor Day, Veterans Day, Thanksgiving, Christmas
  - `_es_holidays(year)` — Spanish national holidays (Valencia): 10 fixed dates including Valencian Community holidays
  - `_it_holidays(year)` — Italian national holidays (Naples): 11 dates including San Gennaro (Naples patron saint)
  - `season_name` mapped from calendar month (Northern hemisphere)

#### `schema/05_operations.sql` — New file
- **`ALTER TABLE dim_date`** adds three columns idempotently (`ADD COLUMN IF NOT EXISTS`):
  - `is_public_holiday SMALLINT` — 1 if date is a public holiday in any study-port country
  - `holiday_name VARCHAR(255)` — pipe-separated holiday names (e.g. `Christmas Day | Navidad | Natale`)
  - `season_name VARCHAR(10)` — meteorological season (Winter/Spring/Summer/Autumn)
- **`container_event_type` ENUM** — `gate-in / discharge / load / shift / gate-out / maintenance-block / release`
- **`yard_block_type` ENUM** — `import / export / reefer / hazmat / empty`
- **`ops_yard_block`** — yard blocks per terminal with `block_code`, `row_count`, `bay_count`, `tier_count`, `block_type`, FK to `dim_port` and `dim_terminal`
- **`ops_container`** — container master with ISO type, size, lifecycle status, current position (block/row/bay/tier), FKs to `dim_port`, `dim_terminal`, `dim_vessel`, `dim_carrier`; `is_hazardous`, `is_reefer`, `cargo_value_usd`
- **`ops_container_event`** — append-only event log with `container_event_type` ENUM, `from_position`/`to_position` strings, FK to `fact_port_calls` (nullable)
- **`ops_maintenance_block`** — maintenance time-window reservations by block and terminal with `start_time`, `end_time`, `reason`, `operator`, `is_active`
- Indexes on all ops tables: status, port/terminal, carrier, vessel, block, event timestamp, maintenance active windows

#### `api/main.py` — New file
- FastAPI application with `asyncpg` connection pool and `APIKeyHeader` authentication
- **`/containers` router:**
  - `GET /containers/{container_number}` — retrieve container master record
  - `POST /containers` — gate-in a new container (inserts master + gate-in event atomically)
  - `PATCH /containers/{container_number}/position` — shift container to new yard position (updates master + logs shift event atomically)
- **`/events` router:**
  - `POST /events` — record any container lifecycle event; updates `ops_container.status`
  - `GET /events/{container_number}` — retrieve event history (newest first, paginated)
- **`/yard` router:**
  - `GET /yard/blocks/{terminal_key}/occupancy` — real-time block occupancy with capacity and percentage
  - `POST /yard/maintenance` — create a maintenance block reservation
  - `GET /yard/maintenance` — list all currently active maintenance windows (optional `?terminal_key=` filter)
- Full Pydantic v2 request/response models with field validation
- `lifespan` context manager for clean pool startup/shutdown

#### `requirements.txt` — Updated
- Added: `asyncpg==0.29.0`, `fastapi==0.111.0`, `uvicorn[standard]==0.29.0`, `pydantic==2.7.1`, `python-dotenv==1.0.1`, `httpx==0.27.0`

#### `docker-compose.yml` — Updated
- Added `api` service: builds from `Dockerfile.api`, maps port 8000, depends on `postgres` healthcheck, mounts `./api` volume for live-reload
- Added `Dockerfile.api`: multi-stage Python 3.11-slim build

#### `docs/powerbi_connection.md` — New file
- Step-by-step guide: psqlODBC 16.x driver download link, 64-bit System DSN configuration
- DirectQuery recommended settings (max connections, referential integrity, parallel loading)
- Table import list: 6 mart views with recommended Power BI table names
- Suggested data model layout with relationship diagram and cardinality guidance
- Three dashboard page specifications:
  - **Port Manager** — uses `mart_port_comparison_annual` + `mart_port_monthly_throughput`
  - **Cargo Operations** — uses `mart_cargo_by_route_and_direction`
  - **Carrier Intelligence** — uses `mart_carrier_market_share` + `mart_vessel_dwell_analysis`
- Troubleshooting table

---

### Changed

#### `schema/04_views.sql` — Fan-out bug fix in two mart views

**`mart_port_monthly_throughput`:**
- **Root cause:** Original query joined `fact_port_calls` to `fact_cargo_movements` *before* aggregation. Each port call row was multiplied by its N cargo movement rows, inflating `AVG(berth_waiting_hours)`, `AVG(at_berth_hours)`, `AVG(turnaround_hours)`, and `AVG(crane_moves_total)` by the per-call cargo count (~N×).
- **Fix:** Two-CTE pattern — `calls` CTE aggregates efficiency KPIs at port-call grain from `fact_port_calls` alone; `cargo` CTE aggregates throughput totals independently from `fact_cargo_movements`; final SELECT joins both to `dim_port × date spine`. Fact tables never joined to each other before aggregation.

**`mart_port_comparison_annual`:**
- Same fan-out bug and same two-CTE fix applied at annual grain.

**`mart_terminal_performance`:**
- Refactored to two-CTE pattern for consistency and correctness (same underlying risk).

**`mart_vessel_dwell_analysis`:**
- No fan-out fix needed (no cargo join); enhanced with `season_name` and `is_public_holiday` from `dim_date`.

---

#### `models/staging/stg_port_calls.sql` — Enhanced (v2)
- All numeric measures wrapped in `COALESCE(…, 0)` null-safe guards
- `is_weekend` now derived from `dim_date.is_weekend` (JOIN added) rather than `EXTRACT(DOW FROM arrival_datetime)` — ensures alignment with mart views
- `is_public_holiday` and `season_name` surfaced from `dim_date`
- `port_region` added via JOIN to `dim_port`
- `turnaround_band` (label string) already present in v1; ensured null-safe

#### `models/staging/stg_cargo_movements.sql` — Enhanced (v2)
- All numeric measures wrapped in `COALESCE(…, 0)` null-safe guards
- `is_weekend`, `is_public_holiday`, `season_name` added via JOIN to `dim_date`
- `turnaround_band` label added via JOIN to `fact_port_calls` (parent call)
- `port_region` added via JOIN to `dim_port`
- `cargo_value_tier` currency formatting standardised

---

### Unchanged
- All existing table names (`dim_*`, `fact_*`)
- All existing column names on dimension and fact tables
- All existing FK references
- All existing CSV output filenames from `generate_data.py`
- `schema/01_dimensions.sql`, `schema/02_facts.sql`, `schema/03_indexes.sql`
- `scripts/load_data.py`

---

## [1.0.0] — Initial release

- Star schema: 11 dimension tables, 2 fact tables, 5 mart views
- Three study ports: Baltimore, Valencia, Naples
- Synthetic data generator producing ~6,600 port calls and ~100,000 cargo movements across 2022–2024
- Docker Compose with PostgreSQL 16
