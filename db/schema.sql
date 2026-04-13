-- ============================================================================
-- schema.sql — TimescaleDB Schema for Fleet Telemetry
-- ============================================================================
-- This file is auto-run by the TimescaleDB container on first startup via
-- the docker-entrypoint-initdb.d volume mount.
-- ============================================================================

-- Core telemetry table.  One row per ping from a vehicle.
-- Design decisions:
--   • ts as TIMESTAMPTZ: timezone-aware, required by TimescaleDB hypertable
--   • vid + fid as TEXT: keeps the schema simple; UUIDs/ints can be used later
--   • spd/hdg/acc as REAL: float32 is plenty of precision for vehicle telemetry
--   • seq as INTEGER: monotonic sequence per vehicle to detect dropped/out-of-order pings
CREATE TABLE IF NOT EXISTS telemetry (
    ts        TIMESTAMPTZ      NOT NULL,
    vid       TEXT             NOT NULL,
    fid       TEXT,
    lat       DOUBLE PRECISION,
    lng       DOUBLE PRECISION,
    spd       REAL,
    hdg       REAL,
    acc       REAL,
    ign       BOOLEAN,
    seq       INTEGER
);

-- Convert to a hypertable partitioned on ts.  This is the core TimescaleDB
-- feature — it transparently partitions the table into time-based chunks,
-- so range queries like "last 30 minutes" only scan the relevant chunk.
SELECT create_hypertable('telemetry', 'ts', if_not_exists => TRUE);

-- Composite index on (vid, ts DESC) — optimised for the most common query:
-- "give me the latest N pings for vehicle X".
CREATE INDEX IF NOT EXISTS idx_telemetry_vid_ts ON telemetry (vid, ts DESC);
