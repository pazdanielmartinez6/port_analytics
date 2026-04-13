"""
Port Operations Analytics Warehouse — FastAPI TOS Service
=========================================================
Provides a RESTful API for the Terminal Operating System (TOS)
layer, writing to and reading from the ops_* tables defined in
schema/05_operations.sql.

Authentication : X-API-Key header (key stored in .env as API_KEY).

Routers
-------
/containers  — Container master records (gate-in, position update)
/events      — Container event log (discharge, load, shift, etc.)
/yard        — Yard block occupancy and maintenance reservations

Run locally:
    uvicorn api.main:app --reload --port 8000

Environment variables (see .env.example):
    API_KEY           — Secret key for X-API-Key header auth
    POSTGRES_HOST     — Postgres hostname (default: localhost)
    POSTGRES_PORT     — Postgres port    (default: 5432)
    POSTGRES_DB       — Database name    (default: port_analytics)
    POSTGRES_USER     — DB username      (default: portadmin)
    POSTGRES_PASSWORD — DB password
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Optional

import asyncpg
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Security, status
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel, Field, field_validator

load_dotenv()

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────
API_KEY_NAME = "X-API-Key"
API_KEY      = os.getenv("API_KEY", "changeme-set-in-dotenv")

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST",     "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB",       "port_analytics"),
    "user":     os.getenv("POSTGRES_USER",     "portadmin"),
    "password": os.getenv("POSTGRES_PASSWORD", "portpass123"),
    "min_size": 2,
    "max_size": 10,
}

api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=True)


# ─────────────────────────────────────────────────────────────
# Database connection pool (shared across request lifecycle)
# ─────────────────────────────────────────────────────────────
_pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create asyncpg connection pool on startup; close on shutdown."""
    global _pool
    _pool = await asyncpg.create_pool(**DB_CONFIG)
    yield
    await _pool.close()


async def get_pool() -> asyncpg.Pool:
    """FastAPI dependency: returns the shared connection pool."""
    if _pool is None:
        raise RuntimeError("Connection pool not initialised")
    return _pool


# ─────────────────────────────────────────────────────────────
# Auth dependency
# ─────────────────────────────────────────────────────────────
async def verify_api_key(key: str = Security(api_key_header)) -> str:
    """Validate X-API-Key header against the configured secret."""
    if key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or missing API key",
        )
    return key


# ─────────────────────────────────────────────────────────────
# Pydantic models
# ─────────────────────────────────────────────────────────────

class ContainerIn(BaseModel):
    """Request body for POST /containers (gate-in)."""
    container_number: str = Field(
        ..., min_length=11, max_length=11,
        description="ISO 6346 container number, e.g. MSCU1234567"
    )
    iso_type_code:   str   = Field(..., min_length=4, max_length=4)
    size_feet:       int   = Field(..., ge=20, le=48)
    port_key:        int   = Field(..., gt=0)
    terminal_key:    int   = Field(..., gt=0)
    carrier_key:     int   = Field(..., gt=0)
    is_hazardous:    bool  = False
    is_reefer:       bool  = False
    cargo_value_usd: float = Field(0.0, ge=0)
    operator:        str   = Field(..., min_length=1, max_length=100)

    @field_validator("container_number")
    @classmethod
    def uppercase_container(cls, v: str) -> str:
        return v.upper()


class ContainerOut(BaseModel):
    """Response model for container master data."""
    container_number: str
    iso_type_code:    str
    size_feet:        int
    status:           str
    current_block:    Optional[str]
    current_row:      Optional[int]
    current_bay:      Optional[int]
    current_tier:     Optional[int]
    port_key:         int
    terminal_key:     int
    carrier_key:      int
    is_hazardous:     bool
    is_reefer:        bool
    cargo_value_usd:  float
    created_at:       datetime


class ContainerPositionUpdate(BaseModel):
    """Request body for PATCH /containers/{container_number}/position."""
    to_block:  str = Field(..., min_length=1, max_length=10)
    to_row:    int = Field(..., gt=0)
    to_bay:    int = Field(..., gt=0)
    to_tier:   int = Field(..., gt=0)
    operator:  str = Field(..., min_length=1, max_length=100)
    port_call_id: Optional[int] = None


class ContainerEventIn(BaseModel):
    """Request body for POST /events."""
    container_number: str = Field(..., min_length=11, max_length=11)
    event_type:       str = Field(
        ...,
        description=(
            "One of: gate-in, discharge, load, shift, "
            "gate-out, maintenance-block, release"
        )
    )
    from_position:    Optional[str] = None
    to_position:      Optional[str] = None
    operator:         str = Field(..., min_length=1, max_length=100)
    event_timestamp:  datetime
    port_call_id:     Optional[int] = None

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, v: str) -> str:
        allowed = {
            "gate-in", "discharge", "load", "shift",
            "gate-out", "maintenance-block", "release",
        }
        if v not in allowed:
            raise ValueError(f"event_type must be one of {sorted(allowed)}")
        return v

    @field_validator("container_number")
    @classmethod
    def uppercase_container(cls, v: str) -> str:
        return v.upper()


class ContainerEventOut(BaseModel):
    """Response model for a recorded container event."""
    event_id:         int
    container_number: str
    event_type:       str
    from_position:    Optional[str]
    to_position:      Optional[str]
    operator:         str
    event_timestamp:  datetime
    port_call_id:     Optional[int]
    created_at:       datetime


class YardBlockOccupancy(BaseModel):
    """Response model for GET /yard/blocks/{terminal_key}/occupancy."""
    yard_block_id:  int
    terminal_key:   int
    block_code:     str
    block_type:     str
    row_count:      int
    bay_count:      int
    tier_count:     int
    capacity:       int                  # row × bay × tier
    occupied:       int                  # containers currently in block
    occupancy_pct:  Optional[float]      # occupied / capacity × 100


class MaintenanceBlockIn(BaseModel):
    """Request body for POST /yard/maintenance."""
    block_code:   str = Field(..., min_length=1, max_length=10)
    terminal_key: int = Field(..., gt=0)
    start_time:   datetime
    end_time:     datetime
    reason:       str = Field(..., min_length=1, max_length=255)
    operator:     str = Field(..., min_length=1, max_length=100)

    @field_validator("end_time")
    @classmethod
    def end_after_start(cls, v: datetime, info) -> datetime:
        if "start_time" in info.data and v <= info.data["start_time"]:
            raise ValueError("end_time must be after start_time")
        return v


class MaintenanceBlockOut(BaseModel):
    """Response model for a maintenance reservation."""
    maintenance_id: int
    block_code:     str
    terminal_key:   int
    start_time:     datetime
    end_time:       datetime
    reason:         str
    operator:       str
    is_active:      bool
    created_at:     datetime


# ─────────────────────────────────────────────────────────────
# FastAPI app
# ─────────────────────────────────────────────────────────────
app = FastAPI(
    title="Port TOS API",
    description=(
        "Terminal Operating System REST API for the Port Operations "
        "Analytics Warehouse. Writes to ops_* tables in Postgres."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# ─────────────────────────────────────────────────────────────
# Router: /containers
# ─────────────────────────────────────────────────────────────

@app.get(
    "/containers/{container_number}",
    response_model=ContainerOut,
    tags=["containers"],
    summary="Get container by number",
)
async def get_container(
    container_number: str,
    pool: asyncpg.Pool = Depends(get_pool),
    _: str = Depends(verify_api_key),
):
    """Retrieve the master record for a single container."""
    row = await pool.fetchrow(
        """
        SELECT container_number, iso_type_code, size_feet, status,
               current_block, current_row, current_bay, current_tier,
               port_key, terminal_key, carrier_key,
               is_hazardous, is_reefer, cargo_value_usd, created_at
        FROM ops_container
        WHERE container_number = $1
        """,
        container_number.upper(),
    )
    if not row:
        raise HTTPException(status_code=404, detail="Container not found")
    return dict(row)


@app.post(
    "/containers",
    response_model=ContainerOut,
    status_code=status.HTTP_201_CREATED,
    tags=["containers"],
    summary="Gate-in: register a new container",
)
async def create_container(
    body: ContainerIn,
    pool: asyncpg.Pool = Depends(get_pool),
    _: str = Depends(verify_api_key),
):
    """
    Register a new container arriving at gate (gate-in event).
    Creates the ops_container master record and appends a gate-in
    event to ops_container_event in a single transaction.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Insert / upsert container master record
            row = await conn.fetchrow(
                """
                INSERT INTO ops_container (
                    container_number, iso_type_code, size_feet, status,
                    port_key, terminal_key, carrier_key,
                    is_hazardous, is_reefer, cargo_value_usd
                )
                VALUES ($1, $2, $3, 'on-terminal', $4, $5, $6, $7, $8, $9)
                ON CONFLICT (container_number) DO UPDATE
                    SET status       = 'on-terminal',
                        port_key     = EXCLUDED.port_key,
                        terminal_key = EXCLUDED.terminal_key
                RETURNING *
                """,
                body.container_number,
                body.iso_type_code,
                body.size_feet,
                body.port_key,
                body.terminal_key,
                body.carrier_key,
                body.is_hazardous,
                body.is_reefer,
                body.cargo_value_usd,
            )
            # Log gate-in event
            await conn.execute(
                """
                INSERT INTO ops_container_event (
                    container_number, event_type, operator, event_timestamp
                )
                VALUES ($1, 'gate-in', $2, now())
                """,
                body.container_number,
                body.operator,
            )
    return dict(row)


@app.patch(
    "/containers/{container_number}/position",
    response_model=ContainerOut,
    tags=["containers"],
    summary="Shift/move a container to a new yard position",
)
async def update_container_position(
    container_number: str,
    body: ContainerPositionUpdate,
    pool: asyncpg.Pool = Depends(get_pool),
    _: str = Depends(verify_api_key),
):
    """
    Update the physical yard position of a container (shift/move).
    Records the move in ops_container_event and updates the
    current position in ops_container — all in one transaction.
    """
    cn = container_number.upper()
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Fetch current position to populate from_position in event log
            existing = await conn.fetchrow(
                "SELECT current_block, current_row, current_bay, current_tier "
                "FROM ops_container WHERE container_number = $1",
                cn,
            )
            if not existing:
                raise HTTPException(status_code=404, detail="Container not found")

            from_pos = None
            if existing["current_block"]:
                from_pos = (
                    f"{existing['current_block']}:"
                    f"{existing['current_row']}:"
                    f"{existing['current_bay']}:"
                    f"{existing['current_tier']}"
                )
            to_pos = f"{body.to_block}:{body.to_row}:{body.to_bay}:{body.to_tier}"

            # Update master record
            row = await conn.fetchrow(
                """
                UPDATE ops_container
                SET current_block = $1,
                    current_row   = $2,
                    current_bay   = $3,
                    current_tier  = $4,
                    status        = 'in-yard'
                WHERE container_number = $5
                RETURNING *
                """,
                body.to_block, body.to_row, body.to_bay, body.to_tier, cn,
            )

            # Log shift event
            await conn.execute(
                """
                INSERT INTO ops_container_event (
                    container_number, event_type, from_position, to_position,
                    operator, event_timestamp, port_call_id
                )
                VALUES ($1, 'shift', $2, $3, $4, now(), $5)
                """,
                cn, from_pos, to_pos, body.operator, body.port_call_id,
            )
    return dict(row)


# ─────────────────────────────────────────────────────────────
# Router: /events
# ─────────────────────────────────────────────────────────────

@app.post(
    "/events",
    response_model=ContainerEventOut,
    status_code=status.HTTP_201_CREATED,
    tags=["events"],
    summary="Record a container event",
)
async def create_event(
    body: ContainerEventIn,
    pool: asyncpg.Pool = Depends(get_pool),
    _: str = Depends(verify_api_key),
):
    """
    Append a container lifecycle event to ops_container_event.
    Also updates ops_container.status to reflect the event type.
    """
    # Map event_type → container status
    STATUS_MAP = {
        "gate-in":           "on-terminal",
        "discharge":         "at-quay",
        "load":              "loaded",
        "shift":             "in-yard",
        "gate-out":          "departed",
        "maintenance-block": "in-yard",
        "release":           "on-terminal",
    }
    new_status = STATUS_MAP[body.event_type]

    async with pool.acquire() as conn:
        async with conn.transaction():
            # Verify container exists
            exists = await conn.fetchval(
                "SELECT 1 FROM ops_container WHERE container_number = $1",
                body.container_number,
            )
            if not exists:
                raise HTTPException(
                    status_code=404,
                    detail=f"Container {body.container_number} not found. "
                           "Create it first via POST /containers.",
                )

            # Insert event
            row = await conn.fetchrow(
                """
                INSERT INTO ops_container_event (
                    container_number, port_call_id, event_type,
                    from_position, to_position,
                    operator, event_timestamp
                )
                VALUES ($1, $2, $3::container_event_type, $4, $5, $6, $7)
                RETURNING *
                """,
                body.container_number,
                body.port_call_id,
                body.event_type,
                body.from_position,
                body.to_position,
                body.operator,
                body.event_timestamp,
            )

            # Update container status
            await conn.execute(
                "UPDATE ops_container SET status = $1 WHERE container_number = $2",
                new_status, body.container_number,
            )
    return dict(row)


@app.get(
    "/events/{container_number}",
    response_model=List[ContainerEventOut],
    tags=["events"],
    summary="Get event history for a container",
)
async def get_events(
    container_number: str,
    limit: int = 50,
    pool: asyncpg.Pool = Depends(get_pool),
    _: str = Depends(verify_api_key),
):
    """Return the full event history for a container, newest first."""
    rows = await pool.fetch(
        """
        SELECT event_id, container_number, event_type, from_position,
               to_position, operator, event_timestamp, port_call_id, created_at
        FROM ops_container_event
        WHERE container_number = $1
        ORDER BY event_timestamp DESC
        LIMIT $2
        """,
        container_number.upper(),
        limit,
    )
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────
# Router: /yard
# ─────────────────────────────────────────────────────────────

@app.get(
    "/yard/blocks/{terminal_key}/occupancy",
    response_model=List[YardBlockOccupancy],
    tags=["yard"],
    summary="Get yard block occupancy for a terminal",
)
async def get_yard_occupancy(
    terminal_key: int,
    pool: asyncpg.Pool = Depends(get_pool),
    _: str = Depends(verify_api_key),
):
    """
    Return occupancy statistics for every yard block in a terminal.
    Capacity = row_count × bay_count × tier_count.
    Occupied  = containers with current_block = block_code.
    """
    rows = await pool.fetch(
        """
        SELECT
            oyb.yard_block_id,
            oyb.terminal_key,
            oyb.block_code,
            oyb.block_type::TEXT,
            oyb.row_count,
            oyb.bay_count,
            oyb.tier_count,
            oyb.row_count * oyb.bay_count * oyb.tier_count      AS capacity,
            COUNT(oc.container_number)                           AS occupied,
            ROUND(
                COUNT(oc.container_number)::NUMERIC
                / NULLIF(oyb.row_count * oyb.bay_count * oyb.tier_count, 0)
                * 100, 1
            )                                                    AS occupancy_pct
        FROM ops_yard_block oyb
        LEFT JOIN ops_container oc
               ON oc.current_block  = oyb.block_code
              AND oc.terminal_key   = oyb.terminal_key
              AND oc.status NOT IN ('departed', 'loaded')
        WHERE oyb.terminal_key = $1
          AND oyb.is_active    = TRUE
        GROUP BY oyb.yard_block_id, oyb.terminal_key,
                 oyb.block_code, oyb.block_type,
                 oyb.row_count, oyb.bay_count, oyb.tier_count
        ORDER BY oyb.block_code
        """,
        terminal_key,
    )
    return [dict(r) for r in rows]


@app.post(
    "/yard/maintenance",
    response_model=MaintenanceBlockOut,
    status_code=status.HTTP_201_CREATED,
    tags=["yard"],
    summary="Create a maintenance block reservation",
)
async def create_maintenance_block(
    body: MaintenanceBlockIn,
    pool: asyncpg.Pool = Depends(get_pool),
    _: str = Depends(verify_api_key),
):
    """
    Reserve a yard block area for a scheduled maintenance window.
    Active reservations prevent new container placement in the block.
    """
    row = await pool.fetchrow(
        """
        INSERT INTO ops_maintenance_block (
            block_code, terminal_key, start_time, end_time, reason, operator
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING *
        """,
        body.block_code,
        body.terminal_key,
        body.start_time,
        body.end_time,
        body.reason,
        body.operator,
    )
    return dict(row)


@app.get(
    "/yard/maintenance",
    response_model=List[MaintenanceBlockOut],
    tags=["yard"],
    summary="Get all active maintenance block reservations",
)
async def get_active_maintenance_blocks(
    terminal_key: Optional[int] = None,
    pool: asyncpg.Pool = Depends(get_pool),
    _: str = Depends(verify_api_key),
):
    """
    Return all maintenance reservations that are currently active
    (is_active = TRUE) and whose time window overlaps the current
    moment. Optionally filter by terminal_key.
    """
    if terminal_key is not None:
        rows = await pool.fetch(
            """
            SELECT maintenance_id, block_code, terminal_key,
                   start_time, end_time, reason, operator, is_active, created_at
            FROM ops_maintenance_block
            WHERE is_active    = TRUE
              AND terminal_key = $1
              AND start_time  <= now()
              AND end_time    >= now()
            ORDER BY start_time
            """,
            terminal_key,
        )
    else:
        rows = await pool.fetch(
            """
            SELECT maintenance_id, block_code, terminal_key,
                   start_time, end_time, reason, operator, is_active, created_at
            FROM ops_maintenance_block
            WHERE is_active  = TRUE
              AND start_time <= now()
              AND end_time   >= now()
            ORDER BY terminal_key, start_time
            """,
        )
    return [dict(r) for r in rows]
