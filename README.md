# 🚀 Distributed Spatio-Temporal Fleet Tracker

A real-time fleet tracking system demonstrating distributed systems architecture for a B.Tech ADBMS capstone project. Tracks 20 simulated vehicles around Mumbai using a modern data pipeline: **MQTT → Kafka → Stream Processor → Redis/TimescaleDB → FastAPI → WebSocket Dashboard**.

---

## 📐 Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────┐     ┌──────────────┐
│   Vehicles   │────▶│  Mosquitto   │────▶│    Kafka     │────▶│Processor │────▶│    Redis     │
│  (Simulator) │     │ (MQTT Broker)│     │  (Stream)    │     │(Consumer)│     │  (Live)      │
└──────────────┘     └──────────────┘     └──────────────┘     └────┬─────┘     └──────┬───────┘
                                                                    │                   │
                                                                    ▼                   ▼
                                                             ┌──────────────┐   ┌──────────────┐
                                                             │ TimescaleDB  │   │   FastAPI    │
                                                             │ (History)    │   │  (REST + WS) │
                                                             └──────────────┘   └──────┬───────┘
                                                                                       │
                                                                                       ▼
                                                                                ┌──────────────┐
                                                                                │  Dashboard   │
                                                                                │ (Leaflet.js) │
                                                                                └──────────────┘
```

## 🗂 Project Structure

```
.
├── docker-compose.yml          # Infrastructure: Mosquitto, Kafka, Zookeeper, Redis, TimescaleDB
├── infra/
│   └── mosquitto.conf          # MQTT broker configuration
├── db/
│   └── schema.sql              # TimescaleDB schema (auto-runs on startup)
├── simulator/
│   └── simulate.py             # Vehicle simulator — 20 threaded vehicles around Mumbai
├── bridge/
│   └── mqtt_to_kafka.py        # MQTT → Kafka bridge
├── processor/
│   └── consumer.py             # Kafka consumer → Redis + TimescaleDB + geofence checks
├── api/
│   └── main.py                 # FastAPI backend (REST + WebSocket + system stats)
├── frontend/
│   ├── index.html              # Live map dashboard (Leaflet + CartoDB Dark Matter)
│   └── dataflow.html           # Data flow visualiser with animated pipeline
├── requirements.txt            # Python dependencies
└── README.md                   # This file
```

## 🛠 Prerequisites

- **Docker & Docker Compose** — for infrastructure services
- **Python 3.11+** — for application services
- No API keys or paid services required — the map uses free CartoDB/OpenStreetMap tiles

---

## 🚀 Quick Start

### Step 1: Start Infrastructure

```bash
docker-compose up -d
```

Wait ~30 seconds for all services to become healthy:

```bash
docker-compose ps
```

**Expected output:** All 5 services (mosquitto, zookeeper, kafka, redis, timescaledb) showing `healthy`.

### Step 2: Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Step 3: Start Services (each in a separate terminal)

**Terminal 1 — Vehicle Simulator:**
```bash
python simulator/simulate.py
```
Expected: `[SIMULATOR] All 20 vehicles emitting telemetry at 1.0s intervals.`

**Terminal 2 — MQTT→Kafka Bridge:**
```bash
python bridge/mqtt_to_kafka.py
```
Expected: `[BRIDGE] Bridge running. Forwarding fleet/demo/# → fleet-telemetry`

**Terminal 3 — Stream Processor:**
```bash
python processor/consumer.py
```
Expected: `[PROCESSOR] Processing telemetry...` (geofence alerts will appear in this terminal)

**Terminal 4 — FastAPI Backend:**
```bash
python api/main.py
```
Expected: `Uvicorn running on http://0.0.0.0:8000`

### Step 4: Open the Dashboard

1. Open your browser to: **http://localhost:8000/frontend/index.html**
2. You should see a dark-themed map (CartoDB Dark Matter) centred on Mumbai with 20 vehicle markers moving in real-time
3. Click the **"Data Flow Visualiser →"** link at the bottom of the sidebar to see the animated pipeline view

### Step 5: Shutting Down & Cleaning Up

To safely stop the background databases and free up your RAM (keeps your data safe):
```bash
docker-compose down
```

To stop the databases and **wipe all historical data** (starts completely fresh next time):
```bash
docker-compose down -v
```

If you need to reclaim hard-drive space and delete all downloaded Docker images (⚠️ **Warning:** You will need a fast internet connection to re-download ~5GB of images the next time you run `up`):
```bash
docker system prune -a --volumes
```

---

## 📡 API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/vehicles` | GET | All live vehicle positions from Redis |
| `/vehicles/{vid}/history?minutes=30` | GET | Historical pings from TimescaleDB |
| `/vehicles/{vid}/stats` | GET | Avg/max speed, distance (last 30 min) |
| `/system/stats` | GET | System-wide metrics for pipeline visualiser |
| `/geofence/breaches` | GET | Last 50 geofence breach events |
| `/ws/live` | WebSocket | Real-time vehicle positions (1s interval) |

Interactive API docs at: **http://localhost:8000/docs**

---

## 🗺 Dashboard Features

### Main Dashboard (`index.html`)
- 🗺️ **Dark CartoDB map** centred on Mumbai (free, no API key)
- 🚗 **20 vehicle markers** with directional arrows showing heading
- 📊 **Live sidebar** with vehicle list, speeds, and status indicators
- 🔶 **Geofence polygon** (BKC Zone) drawn on map with dashed red border
- 🚨 **Breach alerts** — floating cards when vehicles enter/exit the geofence
- 🔌 **WebSocket status** — green/red indicator showing connection status

### Data Flow Visualiser (`dataflow.html`)
- ⚡ **Animated pipeline** — left-to-right pipeline with 8 nodes
- ✨ **Live particles** — coloured dots flowing through the pipeline per real ping
- 💡 **Node glow** — nodes glow when data is actively flowing, dim when stalled
- 📈 **Per-vehicle sparklines** — 60-second speed history drawn with Canvas
- 🟢 **Status indicators** — green (moving), amber (slow), red (stopped) per vehicle
- 🚨 **Breach panel** — sliding cards with red flash animation

---

## ⚙️ Configuration

All services use environment variables with sensible defaults:

| Variable | Default | Used by |
|---|---|---|
| `MQTT_HOST` | `localhost` | simulator, bridge |
| `MQTT_PORT` | `1883` | simulator, bridge |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | bridge, processor |
| `REDIS_HOST` | `localhost` | processor, API |
| `PG_HOST` | `localhost` | processor, API |
| `PG_USER` | `fleet` | processor, API |
| `PG_PASS` | `fleet` | processor, API |
| `PG_DB` | `fleetdb` | processor, API |
| `NUM_VEHICLES` | `20` | simulator |

---

## 🔧 Troubleshooting

1. **Services crash on startup:** Each Python service has retry loops — they'll wait for infrastructure to be ready. Make sure Docker containers are healthy first.
2. **No data on dashboard:** Check that all 4 Python services are running. Data flows: simulator → bridge → processor → API.
3. **Map doesn't render:** The map uses free CartoDB tiles — check your internet connection. No API key is needed.
4. **Kafka connection errors:** Wait 30-60 seconds after `docker-compose up` for Kafka to fully initialise.

---

## 🎓 Capstone Report Notes

Key distributed systems concepts demonstrated:
- **Event-driven architecture** — MQTT pub/sub + Kafka streams
- **Separation of concerns** — each service handles one responsibility
- **Hot vs cold storage** — Redis (live state, <1ms) vs TimescaleDB (historical, time-partitioned)
- **Geofencing** — computational geometry (point-in-polygon via Shapely) on streaming data
- **WebSocket push** — real-time dashboard updates without polling
- **Hypertable partitioning** — TimescaleDB automatic time-based partitioning for efficient range queries
- **Consumer groups** — Kafka partition-key routing ensures per-vehicle ordering

