#!/usr/bin/env python3
"""
============================================================================
FastAPI Backend — REST API + WebSocket for Fleet Tracker Dashboard
============================================================================
Design decisions:
  • FastAPI: Async by default, auto-generated OpenAPI docs, WebSocket support
    built in.  Ideal for a real-time dashboard backend.
  • Redis for live state: /vehicles reads from Redis (sub-ms) instead of
    querying the database, keeping the dashboard snappy.
  • asyncpg for history: Async Postgres driver gives us non-blocking DB
    queries so the WebSocket loop doesn't stall.
  • WebSocket /ws/live: Pushes all vehicle positions every second.  This is
    simpler than SSE and gives us bidirectional capability if needed later.
  • /system/stats: Aggregates live metrics from Redis and TimescaleDB for
    the data-flow visualiser.
============================================================================
"""

import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5433"))
PG_USER = os.getenv("PG_USER", "fleet")
PG_PASS = os.getenv("PG_PASS", "fleet")
PG_DB = os.getenv("PG_DB", "fleetdb")

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------
redis_client: aioredis.Redis = None
pg_pool: asyncpg.Pool = None

# In-memory stats counters
stats = {
    "active_vehicles": 0,
    "pings_per_sec": 0.0,
    "kafka_lag": 0,
    "processed_total": 0,
    "geofence_breaches": 0,
    "redis_vehicle_keys": 0,
    "db_total_rows": 0,
    "db_rows_per_sec": 0.0,
    "ws_clients": 0,
    "uptime_seconds": 0,
}
_start_time = time.time()
_last_row_count = 0
_connected_ws_clients: set[WebSocket] = set()


# ---------------------------------------------------------------------------
# Background task — updates stats every 5 seconds
# ---------------------------------------------------------------------------
async def _stats_updater():
    """Periodically refresh system stats from Redis and TimescaleDB."""
    global _last_row_count
    while True:
        try:
            # Redis vehicle keys
            keys = await redis_client.keys("vehicle:*")
            stats["redis_vehicle_keys"] = len(keys)
            stats["active_vehicles"] = len(keys)
            stats["pings_per_sec"] = float(len(keys))  # ~1 ping/sec/vehicle

            # Processor stats from Redis (set by processor/consumer.py)
            proc_stats = await redis_client.hgetall("processor:stats")
            if proc_stats:
                stats["processed_total"] = int(proc_stats.get("processed_total", 0))
                stats["geofence_breaches"] = int(proc_stats.get("geofence_breaches", 0))

            # TimescaleDB row count
            async with pg_pool.acquire() as conn:
                row = await conn.fetchrow("SELECT COUNT(*) AS cnt FROM telemetry")
                current_count = row["cnt"]
                stats["db_rows_per_sec"] = round((current_count - _last_row_count) / 5.0, 1)
                _last_row_count = current_count
                stats["db_total_rows"] = current_count

            # WebSocket clients + uptime
            stats["ws_clients"] = len(_connected_ws_clients)
            stats["uptime_seconds"] = int(time.time() - _start_time)

        except Exception as e:
            print(f"[API] Stats update error: {e}")

        await asyncio.sleep(5)


# ---------------------------------------------------------------------------
# Lifespan — startup / shutdown
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, pg_pool

    # Connect to Redis with retry
    while True:
        try:
            redis_client = aioredis.Redis(
                host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
            )
            await redis_client.ping()
            print(f"[API] Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
            break
        except Exception as e:
            print(f"[API] Waiting for Redis... ({e})")
            await asyncio.sleep(2)

    # Connect to TimescaleDB with retry
    while True:
        try:
            pg_pool = await asyncpg.create_pool(
                host=PG_HOST, port=PG_PORT,
                user=PG_USER, password=PG_PASS,
                database=PG_DB,
                min_size=2, max_size=10,
            )
            print(f"[API] Connected to TimescaleDB at {PG_HOST}:{PG_PORT}")
            break
        except Exception as e:
            print(f"[API] Waiting for TimescaleDB... ({e})")
            await asyncio.sleep(2)

    # Start background stats updater
    stats_task = asyncio.create_task(_stats_updater())

    yield

    # Cleanup
    stats_task.cancel()
    await redis_client.close()
    await pg_pool.close()


# ---------------------------------------------------------------------------
# FastAPI App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Fleet Tracker API",
    description="Real-time distributed fleet tracking backend",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — allow the frontend to connect from file:// or any origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve frontend static files
FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.isdir(FRONTEND_DIR):
    app.mount("/frontend", StaticFiles(directory=FRONTEND_DIR), name="frontend")


# ---------------------------------------------------------------------------
# REST Endpoints
# ---------------------------------------------------------------------------
@app.get("/")
async def root():
    """Redirect to the dashboard."""
    return {"message": "Fleet Tracker API", "docs": "/docs", "dashboard": "/frontend/index.html"}


@app.get("/vehicles")
async def get_vehicles():
    """
    Return all live vehicle positions from Redis.
    Scans all vehicle:* keys and returns their hash values.
    """
    keys = await redis_client.keys("vehicle:*")
    vehicles = []
    for key in keys:
        data = await redis_client.hgetall(key)
        if data:
            vehicles.append({
                "vid": data.get("vid"),
                "fid": data.get("fid"),
                "ts": data.get("ts"),
                "lat": float(data.get("lat", 0)),
                "lng": float(data.get("lng", 0)),
                "spd": float(data.get("spd", 0)),
                "hdg": float(data.get("hdg", 0)),
                "acc": float(data.get("acc", 0)),
                "ign": data.get("ign", "True") == "True",
                "seq": int(data.get("seq", 0)),
            })
    # Sort by vehicle ID for consistent ordering
    vehicles.sort(key=lambda v: v["vid"])
    return vehicles


@app.get("/vehicles/{vid}/history")
async def get_vehicle_history(vid: str, minutes: int = 30):
    """
    Query TimescaleDB for the last N minutes of pings for a specific vehicle.
    Uses the composite index on (vid, ts DESC) for efficient lookup.
    """
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT ts, lat, lng, spd, hdg, acc, ign, seq
               FROM telemetry
               WHERE vid = $1 AND ts > NOW() - INTERVAL '1 minute' * $2
               ORDER BY ts DESC
               LIMIT 1000""",
            vid, minutes,
        )
    return [
        {
            "ts": row["ts"].isoformat(),
            "lat": row["lat"],
            "lng": row["lng"],
            "spd": row["spd"],
            "hdg": row["hdg"],
            "acc": row["acc"],
            "ign": row["ign"],
            "seq": row["seq"],
        }
        for row in rows
    ]


@app.get("/vehicles/{vid}/stats")
async def get_vehicle_stats(vid: str):
    """
    Compute aggregate statistics for a vehicle over the last 30 minutes.
    Uses TimescaleDB aggregate functions on the hypertable.
    """
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT
                 AVG(spd)  AS avg_speed,
                 MAX(spd)  AS max_speed,
                 COUNT(*)  AS ping_count
               FROM telemetry
               WHERE vid = $1 AND ts > NOW() - INTERVAL '30 minutes'""",
            vid,
        )
        # Approximate distance using sequential lat/lng differences
        # Haversine would be more accurate but this is fine for a prototype
        dist_row = await conn.fetchrow(
            """SELECT SUM(
                 111.0 * SQRT(
                   POWER(lat - LAG(lat) OVER (ORDER BY ts), 2) +
                   POWER((lng - LAG(lng) OVER (ORDER BY ts)) * COS(RADIANS(lat)), 2)
                 )
               ) AS distance_km
               FROM telemetry
               WHERE vid = $1 AND ts > NOW() - INTERVAL '30 minutes'""",
            vid,
        )

    return {
        "vid": vid,
        "period": "last 30 minutes",
        "avg_speed_kmh": round(float(row["avg_speed"] or 0), 1),
        "max_speed_kmh": round(float(row["max_speed"] or 0), 1),
        "ping_count": row["ping_count"],
        "distance_km": round(float(dist_row["distance_km"] or 0), 2),
    }


@app.get("/system/stats")
async def get_system_stats():
    """
    Return live system-wide metrics for the data-flow visualiser.
    Counters are updated by the background _stats_updater task.
    """
    # Refresh WS client count in real-time
    stats["ws_clients"] = len(_connected_ws_clients)
    stats["uptime_seconds"] = int(time.time() - _start_time)
    return stats


@app.get("/geofence/breaches")
async def get_geofence_breaches():
    """Return the last 50 geofence breach events from Redis."""
    breaches = await redis_client.lrange("geofence:breaches", 0, 49)
    return [json.loads(b) for b in breaches]


# ---------------------------------------------------------------------------
# WebSocket — live vehicle positions every 1 second
# ---------------------------------------------------------------------------
@app.websocket("/ws/live")
async def websocket_live(ws: WebSocket):
    """
    Push all vehicle positions from Redis every second to connected clients.
    This is how the frontend map gets real-time updates.
    """
    await ws.accept()
    _connected_ws_clients.add(ws)
    print(f"[API] WebSocket client connected (total: {len(_connected_ws_clients)})")

    try:
        while True:
            # Fetch all live vehicle positions from Redis
            keys = await redis_client.keys("vehicle:*")
            vehicles = []
            for key in keys:
                data = await redis_client.hgetall(key)
                if data:
                    vehicles.append({
                        "vid": data.get("vid"),
                        "ts": data.get("ts"),
                        "lat": float(data.get("lat", 0)),
                        "lng": float(data.get("lng", 0)),
                        "spd": float(data.get("spd", 0)),
                        "hdg": float(data.get("hdg", 0)),
                        "ign": data.get("ign", "True") == "True",
                    })

            # Also include latest geofence breaches
            breaches = await redis_client.lrange("geofence:breaches", 0, 9)
            breach_list = [json.loads(b) for b in breaches]

            await ws.send_json({
                "type": "positions",
                "vehicles": sorted(vehicles, key=lambda v: v["vid"]),
                "breaches": breach_list,
                "ts": datetime.now(timezone.utc).isoformat(),
            })

            await asyncio.sleep(1)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[API] WebSocket error: {e}")
    finally:
        _connected_ws_clients.discard(ws)
        print(f"[API] WebSocket disconnected (total: {len(_connected_ws_clients)})")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )
