# ⚓ Port Operations Analytics Warehouse
### A full-stack data engineering project simulating real-world port operations — from raw events to live BI dashboards.

---

> **Status:** Stage 1 Complete — Database & API layer live.
> **Stage 2 (in progress):** Power BI DirectQuery live dashboard.
> **Stage 3 (planned):** Mobile / desktop operational app.

---

## 📌 Project Overview

This project builds a production-grade **port operations analytics warehouse** from the ground up. It simulates the kind of data infrastructure that real port authorities and logistics operators rely on to monitor vessel traffic, cargo flows, terminal performance, and carrier behaviour — all in near real time.

The system is designed around three ports of study — **Baltimore (US)**, **Valencia (Spain)**, and **Naples (Italy)** — spanning three years of synthetic operational data (2022–2024), calibrated to realistic port throughput benchmarks.

At its core, this is a demonstration of how **API-driven data collection**, **dimensional database modelling**, and **live business intelligence** can converge into a single, scalable operational platform.

---

## 🗂️ Repository Structure

```
port-analytics/
├── api/                    # FastAPI application (REST endpoints)
├── docs/                   # Documentation & connection guides
├── models/
│   ├── staging/            # stg_port_calls.sql, stg_cargo_movements.sql
│   └── marts/              # mart_port_throughput.sql, mart_vessel_performance.sql
├── schema/
│   ├── 01_dimensions.sql   # All dimension tables (star schema)
│   ├── 02_facts.sql        # fact_port_calls & fact_cargo_movements
│   ├── 03_indexes.sql      # Analytical performance indexes
│   ├── 04_views.sql        # Operational views
│   └── 05_operations.sql   # Stored procedures & operational queries
├── scripts/
│   ├── generate_data.py    # Synthetic data generator
│   └── load_data.py        # Bulk CSV loader via COPY FROM STDIN
├── seeds/                  # Generated CSV seed files
├── analysis/               # Exploratory analysis notebooks
├── docker-compose.yml      # PostgreSQL container setup
├── Dockerfile.api          # API container
├── .env.example            # Environment variables template
└── requirements.txt
```

---

## 🛠️ Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Database** | PostgreSQL 16 | Core analytical warehouse |
| **Containerisation** | Docker / Docker Compose | Reproducible local environment |
| **API** | FastAPI (Python) | REST interface for operational data entry |
| **ORM / DB Driver** | asyncpg, psycopg2 | Async & sync PostgreSQL connectivity |
| **Data Generation** | Python (Faker, NumPy, Pandas) | Synthetic operational data at scale |
| **BI Layer** | Power BI Desktop (DirectQuery) | Live dashboards — Stage 2 |
| **Data Modelling** | Star schema (Kimball methodology) | Optimised for analytical queries |
| **Env Management** | python-dotenv | Credential isolation |

---

## 🏗️ Database Architecture

The warehouse follows a **Kimball-style star schema**, purpose-built for analytical queries across port operations dimensions.

### Dimension Tables

| Table | Description |
|---|---|
| `dim_date` | Full date spine (2022–2024) with fiscal periods, seasons, and public holidays |
| `dim_port` | Three study ports with geographic metadata and annual TEU capacity |
| `dim_terminal` | Nine terminals (3 per port) with berth counts, crane inventory, and draft constraints |
| `dim_vessel` | 200 synthetic vessels with IMO identifiers, size class, flag, and capacity data |
| `dim_carrier` | 15 real-world ocean carriers including alliance membership (2M, Ocean Alliance, THE Alliance) |
| `dim_cargo_type` | 8 cargo categories: containerised, reefer, dry bulk, liquid bulk, RoRo, break bulk, hazardous |
| `dim_route` | 15 named trade lanes with origin/destination regions and average transit days |
| `dim_direction` | Import, Export, Transshipment, Coastal |
| `dim_ship_size_class` | UNCTAD vessel size bands (Feeder → Ultra Large) |
| `dim_country` | 30 major trading-partner countries with regional groupings |
| `dim_incoterm` | All 11 Incoterms 2020 rules with risk-transfer and freight responsibility |

### Fact Tables

**`fact_port_calls`** — *Grain: one row per vessel visit to a terminal*
Records every vessel arrival, berth assignment, and departure with full operational timestamps and derived performance measures: berth waiting hours, at-berth hours, turnaround time, crane productivity, and delay flags.

**`fact_cargo_movements`** — *Grain: one row per cargo batch within a port call*
Captures TEU count, weight, unit count, cargo value, hazardous flag, and direction for each cargo batch processed during a port call. Fully denormalised with all dimension foreign keys for star-schema query performance.

### Staging Layer

Staging views (`stg_port_calls`, `stg_cargo_movements`) apply null-safe COALESCE guards, derive enriched business labels (turnaround bands, waiting bands, cargo value tiers, cargo families), and surface calendar flags (weekends, public holidays, seasons) from `dim_date` as the authoritative source — ensuring consistency across all downstream mart queries.

### Mart Layer

| View | Description |
|---|---|
| `mart_port_throughput` | Monthly port-level KPIs: total calls, delay rates, TEU volumes, import/export ratios |
| `mart_vessel_performance` | Carrier × ship-size efficiency: turnaround times, delay rates, TEU per call |
| `mart_route_analysis` | Route-level cargo flow and congestion index (waiting time as % of route transit time) |
| `mart_port_comparison_annual` | Annual cross-port scorecard |
| `mart_carrier_market_share` | TEU market share by carrier and alliance |
| `mart_vessel_dwell_analysis` | Vessel-level dwell time detail |
| `mart_terminal_performance` | Terminal benchmarking by berth utilisation and crane productivity |
| `mart_cargo_by_route_and_direction` | Route × direction cargo flow analysis |

### Index Strategy

Analytical performance indexes cover all foreign key columns on both fact tables, plus composite indexes on the most common join patterns (`port_key + date_key`, `port_key + cargo_type_key`) and a partial index on `is_delayed = 1` to accelerate delay analysis queries.

---

## 🔌 API Layer

The FastAPI application exposes a RESTful interface designed around the operational events that port staff generate on a daily basis. The API is the data entry point for the system, enabling structured, validated registration of:

- **Container gate-in / gate-out events** — registering container arrivals and departures at terminal gates
- **Cargo movement records** — logging cargo batches by type, direction, weight, and value per port call
- **Vessel port call events** — recording arrivals, berth assignments, and departures with timestamps
- **Yard occupancy updates** — tracking container stack positions and terminal capacity in real time
- **Operational status changes** — updating vessel delay flags, crane productivity readings, and call completion status

All endpoints validate against the dimensional model, ensuring referential integrity before any data reaches the warehouse. The async architecture (asyncpg connection pooling, up to 10 concurrent connections) supports concurrent usage from multiple operators.

---

## 🚀 Stage Roadmap

### ✅ Stage 1 — Data Infrastructure & API (Current)
The foundation is complete: PostgreSQL warehouse with full star schema, staging and mart layers, analytical indexes, and a FastAPI backend that accepts operational data through structured REST endpoints. Synthetic data covers three years of realistic port activity across three study ports (~6,500 port calls, ~100,000 cargo movement records).

---

### 📊 Stage 2 — Power BI Live Dashboard (In Progress)

The next stage connects **Power BI Desktop** to the live PostgreSQL warehouse using **DirectQuery mode** — and the choice of DirectQuery over Import mode is deliberate and architecturally significant.

**Why DirectQuery?**

In Import mode, Power BI pulls a full copy of the data into memory. For a static dataset, this is fine. But for an operational port database where vessel arrivals, cargo movements, and delay flags are being updated continuously, Import mode would immediately go stale. Every refresh cycle introduces a lag — and in a live port environment, a 30-minute-old delay rate is operationally useless.

DirectQuery solves this by sending live SQL queries directly to PostgreSQL every time a dashboard visual refreshes. There is no data copy, no import limit, and no staleness — the number on the KPI card reflects what is actually in the database at that moment.

This matters especially because the mart views are pre-aggregated and indexed. DirectQuery against well-designed mart views performs comparably to Import mode for dashboard interactions, while delivering the near real-time visibility that operational monitoring demands.

**Planned Dashboard Pages:**
- **Port Manager** — Monthly throughput KPIs, delay heatmaps, turnaround trend lines
- **Cargo Operations** — Route × direction flow, cargo type mix, hazardous movement trends
- **Carrier Intelligence** — TEU market share, alliance breakdown, carrier dwell benchmarks

---

### 📱 Stage 3 — Operational Mobile / Desktop App (Planned)

The third stage builds a user-facing application — mobile and desktop — directly on top of the existing API. Port operators, terminal supervisors, and logistics coordinators would use this app to register daily operational events from their phones or laptops, replacing manual spreadsheets or paper-based logging.

Planned features:
- Container gate-in and gate-out registration with QR/barcode scanning
- Vessel arrival and departure event logging with geolocation
- Real-time yard occupancy view by terminal and bay
- Operational alerts for delayed vessels exceeding turnaround thresholds
- Carrier and route-level performance snapshots for on-the-ground decisions

The API is already designed to support this. Stage 3 is purely a front-end build on a stable backend.

---

## 💼 Business Value

This system addresses a real operational problem. Port authorities and terminal operators deal with enormous volumes of daily events — vessel movements, cargo handoffs, equipment utilisation, customs clearance — most of which are tracked in disconnected systems or manual logs. The result is delayed reporting, poor visibility into bottlenecks, and reactive rather than proactive management.

This warehouse changes that by:

- **Centralising operational data** into a single, queryable analytical model
- **Enabling near real-time monitoring** through DirectQuery-connected dashboards
- **Providing decision-ready KPIs** — delay rates, turnaround times, TEU throughput, carrier performance — without analyst intervention
- **Supporting operational traceability** — every cargo batch links back to its vessel, carrier, terminal, route, and date
- **Scaling from three ports to any number** through the dimensional model's design

---

## ⚙️ Getting Started

### Prerequisites
- Docker Desktop
- Python 3.11+
- PostgreSQL client (optional, for direct inspection)

### Quickstart

```bash
# 1. Clone the repository
git clone https://github.com/your-username/port-analytics.git
cd port-analytics

# 2. Copy and configure environment variables
cp .env.example .env

# 3. Start PostgreSQL
docker-compose up -d

# 4. Install Python dependencies
pip install -r requirements.txt

# 5. Generate synthetic data
python scripts/generate_data.py

# 6. Load data into the warehouse
python scripts/load_data.py

# 7. Start the API
uvicorn api.main:app --reload
```

The API will be available at `http://localhost:8000`. Interactive docs at `http://localhost:8000/docs`.

---

## 📎 Power BI Connection

See [`docs/powerbi_connection.md`](docs/powerbi_connection.md) for the full guide to connecting Power BI Desktop in DirectQuery mode, including DSN setup, recommended model layout, and dashboard page specifications.

---

## 📄 Licence

MIT — see `LICENSE` for details.

---

*Built as a portfolio demonstration of end-to-end data engineering, API development, and operational analytics for the maritime logistics domain.*
