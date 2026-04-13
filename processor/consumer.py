#!/usr/bin/env python3
"""
============================================================================
Stream Processor — Consumes telemetry from Kafka, writes to Redis + TimescaleDB
============================================================================
Design decisions:
  • Direct Kafka consumer: We deliberately avoid Flink / Spark Streaming
    because for 20 vehicles at 1 Hz this simple consumer is more than adequate.
    The capstone focuses on the architecture, not on scaling to millions.
  • Redis for live state: We store each vehicle's latest position in a Redis
    hash with a 60-second TTL.  If a vehicle stops publishing, it automatically
    disappears from the "live" view.
  • TimescaleDB for history: Every ping is written to the hypertable for
    historical queries and analytics.
  • Geofencing with Shapely: We define a hardcoded polygon around a Mumbai
    area and check point-in-polygon for each ping.  Breach alerts are printed
    to stdout (a real system would publish to a notification topic).
============================================================================
"""

import json
import os
import time
from datetime import datetime

import psycopg2
import redis
from confluent_kafka import Consumer, KafkaError
from shapely.geometry import Point, Polygon

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "fleet-telemetry")
KAFKA_GROUP = os.getenv("KAFKA_GROUP", "fleet-processor")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5433"))
PG_USER = os.getenv("PG_USER", "fleet")
PG_PASS = os.getenv("PG_PASS", "fleet")
PG_DB = os.getenv("PG_DB", "fleetdb")

# ---------------------------------------------------------------------------
# Geofence — hardcoded polygon around Bandra-Kurla Complex, Mumbai
# This is a ~2 km² area near the business district.  In production this
# would come from a database or config service.
# ---------------------------------------------------------------------------
GEOFENCE_NAME = "BKC Zone"
GEOFENCE_POLYGON = Polygon([
    (72.860, 19.065),
    (72.875, 19.065),
    (72.875, 19.075),
    (72.860, 19.075),
    (72.860, 19.065),
])

# Track last known geofence state per vehicle to detect enter/exit transitions
_vehicle_in_geofence: dict[str, bool] = {}

# ---------------------------------------------------------------------------
# Global counters for /system/stats (shared via Redis pub/sub or file)
# We store them in Redis so the API can read them.
# ---------------------------------------------------------------------------
_processed_count = 0
_breach_count = 0


def _connect_redis() -> redis.Redis:
    """Connect to Redis with retry."""
    while True:
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            r.ping()
            print(f"[PROCESSOR] Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
            return r
        except Exception as e:
            print(f"[PROCESSOR] Waiting for Redis... ({e})")
            time.sleep(2)


def _connect_postgres():
    """Connect to TimescaleDB/Postgres with retry."""
    while True:
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT,
                user=PG_USER, password=PG_PASS,
                dbname=PG_DB,
            )
            conn.autocommit = True
            print(f"[PROCESSOR] Connected to TimescaleDB at {PG_HOST}:{PG_PORT}")
            return conn
        except Exception as e:
            print(f"[PROCESSOR] Waiting for TimescaleDB... ({e})")
            time.sleep(2)


def _create_kafka_consumer() -> Consumer:
    """Create Kafka consumer with retry."""
    while True:
        try:
            c = Consumer({
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "group.id": KAFKA_GROUP,
                "auto.offset.reset": "latest",
                "enable.auto.commit": True,
            })
            c.subscribe([KAFKA_TOPIC])
            print(f"[PROCESSOR] Subscribed to Kafka topic: {KAFKA_TOPIC}")
            return c
        except Exception as e:
            print(f"[PROCESSOR] Waiting for Kafka... ({e})")
            time.sleep(2)


def _check_geofence(vid: str, lat: float, lng: float) -> str | None:
    """
    Check if vehicle entered or exited the geofence.
    Returns "entered", "exited", or None.
    """
    global _breach_count
    point = Point(lng, lat)  # Shapely uses (x, y) = (lng, lat)
    currently_inside = GEOFENCE_POLYGON.contains(point)
    was_inside = _vehicle_in_geofence.get(vid, False)

    _vehicle_in_geofence[vid] = currently_inside

    if currently_inside and not was_inside:
        _breach_count += 1
        return "entered"
    elif not currently_inside and was_inside:
        _breach_count += 1
        return "exited"
    return None


def main():
    global _processed_count, _breach_count

    print("[PROCESSOR] Starting stream processor...")

    r = _connect_redis()
    conn = _connect_postgres()
    cur = conn.cursor()
    consumer = _create_kafka_consumer()

    print("[PROCESSOR] Processing telemetry...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[PROCESSOR] Kafka error: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value().decode("utf-8"))
                vid = data["vid"]
                ts = data["ts"]
                lat = data["lat"]
                lng = data["lng"]
                spd = data["spd"]
                hdg = data["hdg"]
                acc = data["acc"]
                ign = data["ign"]
                seq = data["seq"]
                fid = data.get("fid", "")

                # 1. Write live position to Redis — hash with 60s TTL
                #    This is the "hot" state that the API reads for /vehicles
                redis_key = f"vehicle:{vid}"
                r.hset(redis_key, mapping={
                    "vid": vid,
                    "fid": fid,
                    "ts": ts,
                    "lat": str(lat),
                    "lng": str(lng),
                    "spd": str(spd),
                    "hdg": str(hdg),
                    "acc": str(acc),
                    "ign": str(ign),
                    "seq": str(seq),
                })
                r.expire(redis_key, 60)

                # 2. Write full ping to TimescaleDB
                cur.execute(
                    """INSERT INTO telemetry (ts, vid, fid, lat, lng, spd, hdg, acc, ign, seq)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (ts, vid, fid, lat, lng, spd, hdg, acc, ign, seq),
                )

                # 3. Geofence check
                breach = _check_geofence(vid, lat, lng)
                if breach:
                    alert_msg = f"🚨 GEOFENCE ALERT: Vehicle {vid} {breach} '{GEOFENCE_NAME}' at {ts}"
                    print(alert_msg)
                    # Also publish breach to Redis for the API to pick up
                    breach_data = json.dumps({
                        "vid": vid,
                        "event": breach,
                        "zone": GEOFENCE_NAME,
                        "ts": ts,
                        "lat": lat,
                        "lng": lng,
                    })
                    r.lpush("geofence:breaches", breach_data)
                    r.ltrim("geofence:breaches", 0, 49)  # keep last 50

                _processed_count += 1

                # Update processor stats in Redis every 100 messages
                if _processed_count % 100 == 0:
                    r.hset("processor:stats", mapping={
                        "processed_total": str(_processed_count),
                        "geofence_breaches": str(_breach_count),
                    })

            except Exception as e:
                print(f"[PROCESSOR] Error processing message: {e}")

    except KeyboardInterrupt:
        print("\n[PROCESSOR] Shutting down...")
    finally:
        consumer.close()
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
