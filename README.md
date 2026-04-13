# Port Operations Analytics Warehouse

A Kimball star-schema analytical warehouse modelling three ports — **Baltimore**, **Valencia**, and **Naples** — built on PostgreSQL 16, Python 3.11, and Docker. Includes a FastAPI Terminal Operating System (TOS) layer and Power BI connection guide.

---

## Table of Contents

1. [Project Structure](#1-project-structure)
2. [Prerequisites](#2-prerequisites)
3. [Quick Start](#3-quick-start)
4. [Step-by-Step Setup](#4-step-by-step-setup)
5. [Running the FastAPI Service](#5-running-the-fastapi-service)
6. [Verifying the Data](#6-verifying-the-data)
7. [Environment Variables](#7-environment-variables)
8. [Troubleshooting](#8-troubleshooting)
9. [Common Queries](#9-common-queries)

---

## 1. Project Structure

```
port-analytics/
├── schema/
│   ├── 01_dimensions.sql       # All 11 dimension tables
│   ├── 02_facts.sql            # fact_port_calls, fact_cargo_movements
│   ├── 03_indexes.sql          # Performance indexes
│   ├── 04_views.sql            # 6 mart views (dashboard layer)
│   └── 05_operations.sql       # TOS ops_* tables + dim_date enhancement
├── scripts/
│   ├── generate_data.py        # Synthetic data generator
│   └── load_data.py            # CSV → Postgres loader
├── seeds/                      # Generated CSVs land here (git-ignored)
├── models/
│   ├── staging/
│   │   ├── stg_port_calls.sql
│   │   └── stg_cargo_movements.sql
│   └── marts/
│       ├── mart_port_throughput.sql
│       └── mart_vessel_performance.sql
├── api/
│   └── main.py                 # FastAPI TOS service
├── docs/
│   ├── powerbi_connection.md
│   └── CHANGELOG.md
├── .env                        # Your local secrets (never commit)
├── .env.example                # Template — copy to .env
├── docker-compose.yml
├── Dockerfile.api
└── requirements.txt
```

---

## 2. Prerequisites

| Tool | Version | Check |
|------|---------|-------|
| Docker Desktop | 4.x+ | `docker --version` |
| Docker Compose | 2.x+ | `docker compose version` |
| Python | 3.11+ | `python --version` |
| pip | 23+ | `pip --version` |

> **Windows users:** Use PowerShell or Git Bash. WSL2 recommended for best Docker performance.

---

## 3. Quick Start

If you just want it running as fast as possible:

```bash
# 1. Clone / unzip the project, then cd into it
cd port-analytics

# 2. Copy and configure the environment file
cp .env.example .env          # edit .env if you want custom passwords

# 3. Start Postgres
docker compose up -d postgres

# 4. Install Python dependencies
pip install -r requirements.txt

# 5. Apply the schema
python scripts/apply_schema.py   # OR run SQL files manually (see Section 4.2)

# 6. Generate and load synthetic data
python scripts/generate_data.py
python scripts/load_data.py

# 7. Start the API (optional)
docker compose up -d api
```

Done. Connect Power BI to `localhost:5432 / port_analytics` — see `docs/powerbi_connection.md`.

---

## 4. Step-by-Step Setup

### 4.1 Create the `.env` file

```bash
cp .env.example .env
```

Open `.env` and set values (defaults work fine for local development):

```dotenv
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=port_analytics
POSTGRES_USER=portadmin
POSTGRES_PASSWORD=portpass123
API_KEY=changeme-replace-this-with-a-real-secret
```

> **Never commit `.env` to git.** It is listed in `.gitignore` by default.

---

### 4.2 Start PostgreSQL

```bash
docker compose up -d postgres
```

Wait ~10 seconds, then verify it is healthy:

```bash
docker compose ps
# STATUS should show: healthy
```

If Docker auto-ran the schema files from `schema/` on first boot (via the `docker-entrypoint-initdb.d` volume mount), skip to Section 4.3. Otherwise apply the schema manually:

```bash
# Connect with psql
docker exec -it port_postgres psql -U portadmin -d port_analytics

# Inside psql, run each schema file in order:
\i /docker-entrypoint-initdb.d/01_dimensions.sql
\i /docker-entrypoint-initdb.d/02_facts.sql
\i /docker-entrypoint-initdb.d/03_indexes.sql
\i /docker-entrypoint-initdb.d/04_views.sql
\i /docker-entrypoint-initdb.d/05_operations.sql
\q
```

Or from outside the container using your local psql:

```bash
for f in schema/01_dimensions.sql schema/02_facts.sql schema/03_indexes.sql schema/04_views.sql schema/05_operations.sql; do
  psql -h localhost -U portadmin -d port_analytics -f $f
done
```

---

### 4.3 Install Python Dependencies

```bash
pip install -r requirements.txt
```

If you prefer a virtual environment (recommended):

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

---

### 4.4 Generate Synthetic Data

```bash
python scripts/generate_data.py
```

Expected output:

```
================================================================
Port Analytics — Synthetic Data Generator v2
================================================================

[1/3] Generating dimension tables …
[2/3] Generating fact tables …
[3/3] Writing CSVs …
  ✓  dim_date                        1096 rows  →  seeds/dim_date.csv
  ✓  dim_port                           3 rows  →  seeds/dim_port.csv
  ...
  ✓  fact_port_calls                 6,900 rows  →  seeds/fact_port_calls.csv
  ✓  fact_cargo_movements          101,243 rows  →  seeds/fact_cargo_movements.csv

================================================================
fact_port_calls      :    6,900 rows
fact_cargo_movements :  101,243 rows
Done. Run load_data.py to push CSVs into Postgres.
================================================================
```

> Row counts will vary slightly due to Poisson sampling — this is expected.

---

### 4.5 Load Data into PostgreSQL

```bash
python scripts/load_data.py
```

Expected output:

```
Waiting for Postgres … ready.

Truncating existing data …
  Tables truncated.

Loading seed CSVs …
  ✓  dim_date                        1,096 rows  (0.2s)
  ✓  dim_port                            3 rows  (0.0s)
  ...
  ✓  fact_port_calls                 6,900 rows  (0.5s)
  ✓  fact_cargo_movements          101,243 rows  (3.1s)

Committed. Total rows inserted: 108,462
```

---

## 5. Running the FastAPI Service

### Option A — Docker (recommended)

```bash
docker compose up -d api
```

Test it's alive:

```bash
curl -H "X-API-Key: changeme-replace-this-with-a-real-secret" \
     http://localhost:8000/docs
```

The interactive Swagger UI will open at `http://localhost:8000/docs`.

### Option B — Local uvicorn

```bash
uvicorn api.main:app --reload --port 8000
```

### Available endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/containers/{number}` | Fetch container master record |
| `POST` | `/containers` | Gate-in a new container |
| `PATCH` | `/containers/{number}/position` | Move container in yard |
| `POST` | `/events` | Record any container event |
| `GET` | `/events/{number}` | Get event history for a container |
| `GET` | `/yard/blocks/{terminal_key}/occupancy` | Block occupancy stats |
| `POST` | `/yard/maintenance` | Reserve a block for maintenance |
| `GET` | `/yard/maintenance` | List active maintenance windows |

All endpoints require the `X-API-Key` header matching your `.env` `API_KEY` value.

---

## 6. Verifying the Data

Connect to Postgres and run these sanity checks:

```bash
docker exec -it port_postgres psql -U portadmin -d port_analytics
```

```sql
-- Row counts
SELECT 'fact_port_calls'      AS tbl, COUNT(*) FROM fact_port_calls
UNION ALL
SELECT 'fact_cargo_movements', COUNT(*) FROM fact_cargo_movements
UNION ALL
SELECT 'dim_date',             COUNT(*) FROM dim_date;

-- Check seasonality is realistic (Baltimore should peak Oct-Dec)
SELECT month, COUNT(*) AS calls
FROM fact_port_calls
WHERE port_key = 1 AND year = 2023
GROUP BY month ORDER BY month;

-- Check carrier concentration (MSC/Maersk should dominate routes 4,5)
SELECT dc.carrier_name, dr.route_name, COUNT(*) AS calls
FROM fact_port_calls fpc
JOIN dim_carrier dc ON dc.carrier_key = fpc.carrier_key
JOIN dim_route   dr ON dr.route_key   = fpc.route_key
WHERE fpc.port_key = 2 AND dr.route_key IN (4, 5)
GROUP BY 1, 2 ORDER BY calls DESC LIMIT 10;

-- Check YoY TEU growth
SELECT year, SUM(teu_count) AS total_teu
FROM fact_cargo_movements
GROUP BY year ORDER BY year;

-- Verify fan-out fix: avg_waiting_hours should be < 20 for all ports
SELECT port_name, year, month, avg_waiting_hours, avg_turnaround_hours
FROM mart_port_monthly_throughput
ORDER BY port_name, year, month
LIMIT 20;

-- Check holiday data populated
SELECT full_date, holiday_name, season_name
FROM dim_date
WHERE is_public_holiday = 1
ORDER BY full_date
LIMIT 15;
```

---

## 7. Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | `localhost` | Postgres hostname |
| `POSTGRES_PORT` | `5432` | Postgres port |
| `POSTGRES_DB` | `port_analytics` | Database name |
| `POSTGRES_USER` | `portadmin` | Database username |
| `POSTGRES_PASSWORD` | `portpass123` | Database password |
| `API_KEY` | `changeme-…` | Secret for `X-API-Key` header auth |

---

## 8. Troubleshooting

### Docker / Postgres issues

**Problem:** `docker compose up -d postgres` exits immediately  
**Fix:** Check logs — `docker compose logs postgres`. Most common cause is a port conflict.
```bash
# Check if port 5432 is already in use
lsof -i :5432        # macOS/Linux
netstat -ano | findstr :5432   # Windows
# If occupied, change POSTGRES_PORT in .env to e.g. 5433
```

---

**Problem:** `ERROR: could not connect to server: Connection refused`  
**Fix:** Postgres isn't ready yet. Wait 10–15 seconds after `docker compose up`, then retry. The load script has a built-in retry loop but psql doesn't.
```bash
docker compose ps    # check STATUS is "healthy" not "starting"
```

---

**Problem:** Schema files didn't auto-run (`docker-entrypoint-initdb.d` only runs on first boot)  
**Fix:** If you already started Postgres once with an empty data volume and then added schema files, you need to either reset the volume or run files manually.
```bash
# Nuclear option: wipe the volume and restart
docker compose down -v
docker compose up -d postgres
# Then apply schema manually as shown in Section 4.2
```

---

**Problem:** `psql: error: FATAL: role "portadmin" does not exist`  
**Fix:** The env vars weren't picked up when the container first initialised. Wipe and restart:
```bash
docker compose down -v && docker compose up -d postgres
```

---

### Python / data generation issues

**Problem:** `ModuleNotFoundError: No module named 'numpy'`  
**Fix:**
```bash
pip install -r requirements.txt
# If using a venv, make sure it's activated first
source .venv/bin/activate
```

---

**Problem:** `generate_data.py` runs but produces 0 rows in some tables  
**Fix:** The random seed is fixed at 42 — if you see this it usually means a Python version mismatch affecting NumPy's RNG. Confirm Python 3.11+:
```bash
python --version
```

---

**Problem:** `load_data.py` fails with `ERROR — rolled back: duplicate key value violates unique constraint`  
**Fix:** Data is already loaded. Run with truncate (the default):
```bash
python scripts/load_data.py --truncate
```
Or wipe manually:
```bash
docker exec -it port_postgres psql -U portadmin -d port_analytics \
  -c "TRUNCATE fact_cargo_movements, fact_port_calls, dim_date, dim_port, dim_terminal, dim_ship_size_class, dim_vessel, dim_carrier, dim_cargo_type, dim_direction, dim_country, dim_route, dim_incoterm RESTART IDENTITY CASCADE;"
```

---

**Problem:** `load_data.py` says `SKIP seeds/dim_date.csv not found`  
**Fix:** Run the generator first:
```bash
python scripts/generate_data.py
```

---

### FastAPI / API issues

**Problem:** `403 Forbidden: Invalid or missing API key`  
**Fix:** Add the header to your request. The key must exactly match `API_KEY` in your `.env`:
```bash
curl -H "X-API-Key: your-key-here" http://localhost:8000/containers/MSCU1234567
```

---

**Problem:** API returns `500 Internal Server Error` on first request  
**Fix:** The ops tables may not exist yet. Run `05_operations.sql`:
```bash
docker exec -it port_postgres psql -U portadmin -d port_analytics \
  -f /docker-entrypoint-initdb.d/05_operations.sql
```

---

**Problem:** `RuntimeError: Connection pool not initialised`  
**Fix:** This means uvicorn started before the lifespan event fired — usually a startup crash. Check logs:
```bash
docker compose logs api
```
Most common cause: Postgres not yet healthy when the API container started. The `depends_on: condition: service_healthy` in `docker-compose.yml` should prevent this, but if running uvicorn locally, start Postgres first and wait for it.

---

**Problem:** `asyncpg.exceptions.InvalidPasswordError`  
**Fix:** The `POSTGRES_PASSWORD` in your `.env` doesn't match what Postgres was initialised with. Either update `.env` to match, or reset the volume:
```bash
docker compose down -v && docker compose up -d postgres
```

---

### Mart view issues

**Problem:** `avg_waiting_hours` looks inflated (e.g. > 100 hours)  
**Cause:** You're querying the old `04_views.sql` (v1) which had the fan-out bug.  
**Fix:** Apply the new `04_views.sql`:
```bash
docker exec -it port_postgres psql -U portadmin -d port_analytics \
  -f /docker-entrypoint-initdb.d/04_views.sql
```
Then re-query — values should be under 30 hours for all ports.

---

**Problem:** `holiday_name` column doesn't exist on `dim_date`  
**Cause:** `05_operations.sql` hasn't been run yet.  
**Fix:**
```bash
docker exec -it port_postgres psql -U portadmin -d port_analytics \
  -f /docker-entrypoint-initdb.d/05_operations.sql
```
Then reload `dim_date` data with the new generator which populates those columns:
```bash
python scripts/generate_data.py
python scripts/load_data.py
```

---

**Problem:** Staging views (`stg_port_calls`, `stg_cargo_movements`) return `ERROR: column "season_name" does not exist`  
**Cause:** `05_operations.sql` must run before the staging views to add the new `dim_date` columns.  
**Fix:** Run schema files in order (01 → 05), then recreate views:
```bash
docker exec -it port_postgres psql -U portadmin -d port_analytics \
  -c "\i /docker-entrypoint-initdb.d/05_operations.sql"
```

---

## 9. Common Queries

Copy these into psql or Power BI's Query Editor to validate the data.

```sql
-- Monthly TEU trend with seasonality visible
SELECT port_name, year, month, total_teu, delay_rate_pct
FROM mart_port_monthly_throughput
WHERE year = 2023
ORDER BY port_name, month;

-- Top 5 carriers by TEU share at Valencia
SELECT carrier_name, alliance, teu_share_pct
FROM mart_carrier_market_share
WHERE port_name = 'Port of Valencia' AND year = 2024
ORDER BY teu_share_pct DESC
LIMIT 5;



-- Year-on-year TEU growth (should show ~3-5% per year)
SELECT year,
       SUM(teu_count) AS total_teu,
       ROUND(
           (SUM(teu_count) - LAG(SUM(teu_count)) OVER (ORDER BY year))
           / NULLIF(LAG(SUM(teu_count)) OVER (ORDER BY year), 0) * 100, 1
       ) AS yoy_growth_pct
FROM fact_cargo_movements
GROUP BY year ORDER BY year;

-- Check ops tables exist and are empty (ready for API writes)
SELECT 'ops_yard_block'      AS tbl, COUNT(*) FROM ops_yard_block
UNION ALL
SELECT 'ops_container',              COUNT(*) FROM ops_container
UNION ALL
SELECT 'ops_container_event',        COUNT(*) FROM ops_container_event
UNION ALL
SELECT 'ops_maintenance_block',      COUNT(*) FROM ops_maintenance_block;
```
