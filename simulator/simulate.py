#!/usr/bin/env python3
"""
============================================================================
Vehicle Simulator — Spawns 20 virtual vehicles roaming REAL Mumbai roads!
============================================================================
"""

import json
import math
import os
import random
import threading
import time
import urllib.request
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
FLEET_ID = os.getenv("FLEET_ID", "demo")
NUM_VEHICLES = int(os.getenv("NUM_VEHICLES", "20"))
PUBLISH_INTERVAL = float(os.getenv("PUBLISH_INTERVAL", "1.0"))  # seconds

# Mumbai bounds (tighter to avoid the Arabian Sea)
MIN_LAT, MAX_LAT = 19.00, 19.12
MIN_LNG, MAX_LNG = 72.84, 72.92

# Realistic fallback route in case the routing API completely fails
FALLBACK_ROUTE = [
    [72.86000, 19.06500], [72.86100, 19.06700], [72.86300, 19.07000],
    [72.86500, 19.07200], [72.86600, 19.07500], [72.86800, 19.07700]
]

def haversine(lon1, lat1, lon2, lat2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))

def calculate_heading(lon1, lat1, lon2, lat2):
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlon = lon2 - lon1
    x = math.sin(dlon) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - (math.sin(lat1) * math.cos(lat2) * math.cos(dlon))
    return (math.degrees(math.atan2(x, y)) + 360) % 360

def fetch_osrm_route():
    """Fetch a real road path from OSRM between two points."""
    for attempt in range(10):
        try:
            lat1, lng1 = random.uniform(MIN_LAT, MAX_LAT), random.uniform(MIN_LNG, MAX_LNG)
            lat2, lng2 = random.uniform(MIN_LAT, MAX_LAT), random.uniform(MIN_LNG, MAX_LNG)
            url = f"http://router.project-osrm.org/route/v1/driving/{lng1},{lat1};{lng2},{lat2}?overview=full&geometries=geojson"
            
            req = urllib.request.Request(url, headers={'User-Agent': 'FleetTrackerDemo/1.0'})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode())
                if data.get("code") == "Ok" and len(data["routes"]) > 0:
                    coords = data["routes"][0]["geometry"]["coordinates"]
                    if len(coords) > 5:  # Ensure it's a decent length route
                        return coords
        except Exception:
            pass
        time.sleep(1)
    
    # Fallback to an actual hardcoded road instead of a straight line
    return FALLBACK_ROUTE

def _connect_mqtt() -> mqtt.Client:
    client = mqtt.Client(client_id=f"simulator-{random.randint(0, 99999)}")
    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            client.loop_start()
            print(f"[SIMULATOR] Connected to MQTT broker at {MQTT_HOST}:{MQTT_PORT}")
            return client
        except Exception as e:
            print(f"[SIMULATOR] Waiting for MQTT broker... ({e})")
            time.sleep(2)

def _simulate_vehicle(client: mqtt.Client, vid: str, route_coords: list):
    """
    Interpolates movement along an actual road polyline from OSRM.
    Vehicle reverses direction when it hits the end of the route.
    """
    topic = f"fleet/{FLEET_ID}/vehicle/{vid}/location"
    
    # Pre-calculate segments array
    segments = []
    total_dist = 0.0
    for i in range(len(route_coords) - 1):
        lon1, lat1 = route_coords[i]
        lon2, lat2 = route_coords[i+1]
        dist = haversine(lon1, lat1, lon2, lat2)
        hdg = calculate_heading(lon1, lat1, lon2, lat2)
        segments.append({'dist': dist, 'hdg': hdg, 'p1': (lon1, lat1), 'p2': (lon2, lat2)})
        total_dist += dist
        
    current_dist = random.uniform(0, total_dist)
    direction = 1 # 1 = forward, -1 = backward
    seq = 0

    stop_countdown = random.randint(30, 90)
    stop_duration = 0
    spd = random.uniform(20, 60)

    while True:
        ts = datetime.now(timezone.utc).isoformat()
        ign = True

        if stop_duration > 0:
            spd = 0.0
            ign = False
            stop_duration -= 1
            if stop_duration == 0:
                ign = True
                spd = random.uniform(20, 60)
                stop_countdown = random.randint(30, 90)
        else:
            spd += random.uniform(-2, 2)
            spd = max(10, min(60, spd))
            
            # Convert speed km/h to m/s
            spd_mps = spd * (1000.0 / 3600.0)
            current_dist += spd_mps * direction
            
            # Bounce at route ends
            if current_dist >= total_dist:
                current_dist = total_dist
                direction = -1
            elif current_dist <= 0:
                current_dist = 0
                direction = 1
                
            stop_countdown -= 1
            if stop_countdown <= 0:
                stop_duration = random.randint(5, 10)

        # Map current_dist to a lat/lng on the polyline
        d = 0.0
        active_seg = segments[0]
        for seg in segments:
            if d + seg['dist'] >= current_dist or seg == segments[-1]:
                active_seg = seg
                break
            d += seg['dist']
            
        ratio = (current_dist - d) / active_seg['dist'] if active_seg['dist'] > 0 else 0
        ratio = max(0.0, min(1.0, ratio)) # clamp
        
        lng = active_seg['p1'][0] + ratio * (active_seg['p2'][0] - active_seg['p1'][0])
        lat = active_seg['p1'][1] + ratio * (active_seg['p2'][1] - active_seg['p1'][1])
        
        hdg = active_seg['hdg']
        if direction == -1:
            hdg = (hdg + 180) % 360
            
        acc = round(random.uniform(-0.5, 0.5), 2) if ign else 0.0

        payload = {
            "vid": vid,
            "fid": FLEET_ID,
            "ts": ts,
            "lat": round(lat, 6),
            "lng": round(lng, 6),
            "spd": round(spd, 1),
            "hdg": round(hdg, 1),
            "acc": acc,
            "ign": ign,
            "seq": seq,
        }

        client.publish(topic, json.dumps(payload), qos=0)
        seq += 1

        time.sleep(PUBLISH_INTERVAL)

def main():
    print(f"[SIMULATOR] Fetching real road routes for {NUM_VEHICLES} vehicles from OSRM...")
    routes = []
    for i in range(NUM_VEHICLES):
        routes.append(fetch_osrm_route())
        print(f"  → Fetched route {i+1}/{NUM_VEHICLES}")
        time.sleep(0.1) # Be nice to public API
        
    client = _connect_mqtt()

    threads = []
    for i in range(NUM_VEHICLES):
        vid = f"V{i + 1:03d}"
        t = threading.Thread(target=_simulate_vehicle, args=(client, vid, routes[i]), daemon=True)
        t.start()
        threads.append(t)
        print(f"  → Vehicle {vid} started on route")

    print(f"[SIMULATOR] All {NUM_VEHICLES} vehicles emitting telemetry at {PUBLISH_INTERVAL}s intervals.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[SIMULATOR] Shutting down...")
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()
