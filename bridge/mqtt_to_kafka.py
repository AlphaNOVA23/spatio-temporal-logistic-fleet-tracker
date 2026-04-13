#!/usr/bin/env python3
"""
============================================================================
MQTT-to-Kafka Bridge — Forwards vehicle telemetry from MQTT to Kafka
============================================================================
Design decisions:
  • This bridge exists because MQTT is optimised for device-to-broker
    communication (lightweight, pub/sub), while Kafka is optimised for
    durable, partitioned, replayable event streams.  The bridge translates
    between these two paradigms.
  • Partition key = vehicle_id ensures all pings from the same vehicle land
    in the same Kafka partition, preserving per-vehicle ordering.
  • We use confluent-kafka (librdkafka wrapper) rather than kafka-python
    because it has significantly higher throughput and lower latency.
============================================================================
"""

import json
import os
import time

import paho.mqtt.client as mqtt
from confluent_kafka import Producer

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "fleet/demo/#")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "fleet-telemetry")

# ---------------------------------------------------------------------------
# Kafka Producer — initialised lazily after connectivity check
# ---------------------------------------------------------------------------
producer: Producer = None


def _init_kafka_producer() -> Producer:
    """Create Kafka producer with retry loop until broker is reachable."""
    while True:
        try:
            p = Producer({
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                # Batching config — balance latency vs throughput
                "linger.ms": 5,          # wait up to 5ms to batch messages
                "batch.num.messages": 100,
                "queue.buffering.max.messages": 100000,
            })
            # Test connectivity by requesting metadata
            p.list_topics(timeout=5)
            print(f"[BRIDGE] Connected to Kafka at {KAFKA_BOOTSTRAP}")
            return p
        except Exception as e:
            print(f"[BRIDGE] Waiting for Kafka broker... ({e})")
            time.sleep(2)


def _delivery_report(err, msg):
    """Called once per message to report delivery status."""
    if err is not None:
        print(f"[BRIDGE] Delivery failed: {err}")


# ---------------------------------------------------------------------------
# MQTT callbacks
# ---------------------------------------------------------------------------
def on_connect(client, userdata, flags, rc):
    """Subscribe on (re-)connect so we survive broker restarts."""
    if rc == 0:
        print(f"[BRIDGE] Connected to MQTT broker, subscribing to {MQTT_TOPIC}")
        client.subscribe(MQTT_TOPIC, qos=0)
    else:
        print(f"[BRIDGE] MQTT connection failed with code {rc}")


def on_message(client, userdata, msg):
    """Forward every MQTT message to Kafka.  Partition key = vehicle ID."""
    global producer
    try:
        payload = msg.payload.decode("utf-8")
        data = json.loads(payload)
        vid = data.get("vid", "unknown")

        # Produce to Kafka — key ensures per-vehicle partition affinity
        producer.produce(
            topic=KAFKA_TOPIC,
            key=vid.encode("utf-8"),
            value=payload.encode("utf-8"),
            callback=_delivery_report,
        )
        # Trigger delivery callbacks without blocking
        producer.poll(0)

    except Exception as e:
        print(f"[BRIDGE] Error forwarding message: {e}")


def main():
    global producer

    print("[BRIDGE] Starting MQTT → Kafka bridge...")
    producer = _init_kafka_producer()

    # Connect to MQTT with retry
    mqtt_client = mqtt.Client(client_id="mqtt-kafka-bridge")
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    while True:
        try:
            mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            break
        except Exception as e:
            print(f"[BRIDGE] Waiting for MQTT broker... ({e})")
            time.sleep(2)

    print("[BRIDGE] Bridge running.  Forwarding fleet/demo/# → fleet-telemetry")

    try:
        mqtt_client.loop_forever()
    except KeyboardInterrupt:
        print("\n[BRIDGE] Shutting down...")
        producer.flush(timeout=5)
        mqtt_client.disconnect()


if __name__ == "__main__":
    main()
