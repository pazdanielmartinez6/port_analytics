# Power BI Desktop — PostgreSQL Connection Guide
## Port Operations Analytics Warehouse

> **Target audience:** Port analysts and BI developers connecting Power BI Desktop to the local or Docker-hosted PostgreSQL instance.

---

## 1. Prerequisites

| Item | Requirement |
|------|-------------|
| Power BI Desktop | Version 2.128 or later (March 2024+) |
| PostgreSQL ODBC Driver | psqlODBC 16.x (64-bit) |
| Network access | Port 5432 open on the Postgres host |
| DB credentials | `portadmin` / value from `.env` |

---

## 2. Install the PostgreSQL ODBC Driver

1. Download the **64-bit psqlODBC installer** from the official source:  
   `https://www.postgresql.org/ftp/odbc/versions/msi/`  
   → Choose `psqlodbc_16_xx_xxxx-x64.zip` (latest patch of 16.x).

2. Run the installer with default options.

3. Verify installation:  
   *Control Panel → Administrative Tools → ODBC Data Sources (64-bit)*  
   → **Drivers** tab should list `PostgreSQL Unicode(x64)`.

---

## 3. Create a System DSN

> A System DSN makes the connection available to all Windows users and is required for DirectQuery.

1. Open **ODBC Data Sources (64-bit)** → **System DSN** tab → **Add**.
2. Select **PostgreSQL Unicode(x64)** → **Finish**.
3. Fill in the fields:

| Field | Value |
|-------|-------|
| Data Source | `port_analytics_dsn` |
| Database | `port_analytics` |
| Server | `localhost` (or Docker host IP) |
| Port | `5432` |
| User Name | `portadmin` |
| Password | *(from `.env` POSTGRES_PASSWORD)* |
| SSL Mode | `disable` (local Docker) or `require` (remote) |

4. Click **Test** → should return *Connection successful*.
5. Click **Save**.

---

## 4. Connect Power BI Desktop

1. Open Power BI Desktop → **Home** → **Get data** → search **PostgreSQL** → **Connect**.
2. Enter:
   - **Server:** `localhost`
   - **Database:** `port_analytics`
3. Select **DirectQuery** mode (see Section 5 for rationale).
4. Authentication: **Database** → Username: `portadmin`, Password from `.env`.
5. Click **Connect**.

> **Alternative via ODBC:** Get data → **ODBC** → select `port_analytics_dsn`. Use this if the native PostgreSQL connector prompts for Npgsql installation.

---

## 5. Recommended DirectQuery Settings

DirectQuery sends live SQL to Postgres — no data copy into Power BI. Recommended for this dataset because cargo movements exceed Power BI's 1 GB import limit at full scale.

In Power BI Desktop → **File → Options → Current file → DirectQuery**:

| Setting | Value | Reason |
|---------|-------|--------|
| Maximum connections per data source | **10** | Matches `max_size` in asyncpg pool |
| Assume referential integrity | **ON** | All FK constraints are enforced in schema |
| Treat empty string as null | **ON** | Safer for VARCHAR measures |
| Enable parallel loading | **ON** | Speeds up dashboard page refresh |

In the **Query reduction** section:
- Set **Cross-filtering direction** to **Single** on fact-to-dimension relationships (avoids bi-directional fan-out).
- Enable **Add an Apply button to each slicer** to reduce per-keystroke queries.

---

## 6. Import These Views as Tables

In the Power BI Navigator, expand `port_analytics` → `public` → select these views:

| Power BI Table Name | Source View | Primary Use |
|---------------------|-------------|-------------|
| `PortMonthlyThroughput` | `mart_port_monthly_throughput` | Time-series KPIs |
| `PortComparisonAnnual` | `mart_port_comparison_annual` | Executive scorecard |
| `CargoByRouteDirection` | `mart_cargo_by_route_and_direction` | Route analysis |
| `CarrierMarketShare` | `mart_carrier_market_share` | Market share |
| `VesselDwellAnalysis` | `mart_vessel_dwell_analysis` | Dwell time detail |
| `TerminalPerformance` | `mart_terminal_performance` | Terminal benchmarking |

> **Do not import raw fact tables** in DirectQuery mode — always use mart views which are pre-aggregated and validated.

---

## 7. Suggested Data Model Layout

In the **Model view** (diagram), arrange tables as follows:

```
[PortComparisonAnnual]      [PortMonthlyThroughput]
          │                          │
          └──── port_name ───────────┘
                    │
         [CarrierMarketShare]
                    │
              carrier_name
                    │
         [VesselDwellAnalysis]  ── port_call_id ── [CargoByRouteDirection]
```

**Relationships to create manually** (since views share no true PKs):

| From | To | Column | Cardinality |
|------|----|--------|-------------|
| `PortMonthlyThroughput` | `PortComparisonAnnual` | `port_name` + `year` | Many:1 |
| `VesselDwellAnalysis` | `CarrierMarketShare` | `carrier_name` + `year` | Many:1 |
| `CargoByRouteDirection` | `PortMonthlyThroughput` | `port_name` + `year` + `quarter` | Many:1 |

Add a standalone **DateTable** (using Power BI's built-in date table or `dim_date`) and relate it to each mart view on `year` + `month`.

---

## 8. Dashboard Page Specifications

### Page 1 — Port Manager

**Data sources:** `PortComparisonAnnual`, `PortMonthlyThroughput`

| Visual | Type | Fields |
|--------|------|--------|
| KPI cards | Card | `total_teu`, `total_calls`, `delay_rate_pct`, `avg_turnaround_hours` |
| Annual TEU comparison | Clustered bar | `port_name` × `total_teu` × `year` (color by year) |
| Monthly throughput trend | Line chart | `month_name` × `total_teu`, slicer on `port_name` and `year` |
| Capacity utilisation | Gauge | `capacity_utilisation_pct` per port |
| Delay heatmap | Matrix | `port_name` (rows) × `month_name` (cols) × `delay_rate_pct` (values, conditional format) |

**Slicers:** Year, Port Name, Quarter

---

### Page 2 — Cargo Operations

**Data source:** `CargoByRouteDirection`

| Visual | Type | Fields |
|--------|------|--------|
| TEU by direction | Donut chart | `direction_name` × `total_teu` |
| Top routes by TEU | Horizontal bar | `route_name` × `total_teu`, top 10 filter |
| Cargo type mix | Stacked bar | `cargo_category` × `total_teu`, grouped by `port_name` |
| Value vs weight scatter | Scatter | X = `total_weight_tonnes`, Y = `total_cargo_value_usd`, size = `movement_count`, color = `port_name` |
| Hazardous movements trend | Line | `quarter` × `hazardous_movement_count` per port |

**Slicers:** Port Name, Year, Cargo Category, Direction

---

### Page 3 — Carrier Intelligence

**Data sources:** `CarrierMarketShare`, `VesselDwellAnalysis`

| Visual | Type | Fields |
|--------|------|--------|
| TEU market share | Pie / treemap | `carrier_name` × `teu_share_pct` (filter: latest year) |
| Alliance share | Donut | `alliance` × `carrier_teu` |
| Carrier dwell benchmark | Clustered bar | `carrier_name` × `avg_turnaround_hours` from `VesselDwellAnalysis` |
| Delay rate by carrier | Bar (descending) | `carrier_name` × `is_delayed` avg from `VesselDwellAnalysis` |
| Year-on-year share change | Line | `year` × `teu_share_pct`, one line per top-5 carrier |
| Vessel size class breakdown | Stacked bar | `ship_size_class` × `total_teu`, grouped by `carrier_name` |

**Slicers:** Year, Port Name, Alliance

---

## 9. Performance Tips

- **Enable query caching** (File → Options → Report settings → Query caching → On).
- Add a **date range slicer** to every page and set a default to the last 12 months — this dramatically reduces the row scan on mart views.
- For the `VesselDwellAnalysis` view (row-level grain), add a **Top N filter** by `turnaround_hours` when used in scatter charts to avoid > 10k mark rendering.
- Consider **Composite model** (DirectQuery + Import) for `dim_date`, `dim_port`, `dim_carrier` — these are small, rarely change, and import mode gives faster slicer response.

---

## 10. Troubleshooting

| Problem | Fix |
|---------|-----|
| *"Unable to connect"* | Verify Docker is running: `docker ps \| grep port_postgres` |
| *"Driver not found"* | Confirm 64-bit psqlODBC installed; Power BI requires 64-bit |
| *"SSL error"* | Set SSL Mode to `disable` in DSN for local Docker |
| Slow DirectQuery | Check `pg_stat_activity`; ensure mart view indexes exist (run `03_indexes.sql`) |
| Empty data in visuals | Run `python scripts/load_data.py` and verify row counts in Postgres |
