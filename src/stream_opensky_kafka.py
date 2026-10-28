"""Phase 2: Standalone live stream producer that polls the OpenSky REST API
every 15 seconds for European airspace state vectors and sends the JSON payload to Kafka.
"""
import argparse
import json
import os
import time
import requests
from kafka import KafkaProducer

OPENSKY_STATES_URL = "https://opensky-network.org/api/states/all"
# Bounding box covering European airspace
EU_BBOX = {
    "lamin": 35.0,
    "lomin": -12.0,
    "lamax": 62.0,
    "lomax": 30.0,
}
KAFKA_TOPIC = "opensky-live-states"
POLL_INTERVAL_SECONDS = 15


def fetch_live_states() -> dict:
    auth = None
    user = os.getenv("OPENSKY_USERNAME")
    pw = os.getenv("OPENSKY_PASSWORD")
    if user and pw:
        auth = (user, pw)

    resp = requests.get(OPENSKY_STATES_URL, params=EU_BBOX, auth=auth, timeout=10)
    resp.raise_for_status()
    return resp.json()


def stream_to_kafka(bootstrap_servers: str = "localhost:9092", max_iterations: int | None = None):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"Kafka producer connected to {bootstrap_servers}. Streaming to topic '{KAFKA_TOPIC}' every {POLL_INTERVAL_SECONDS}s...")

    iteration = 0
    try:
        while max_iterations is None or iteration < max_iterations:
            iteration += 1
            try:
                payload = fetch_live_states()
                states_count = len(payload.get("states") or [])
                future = producer.send(KAFKA_TOPIC, value=payload)
                meta = future.get(timeout=10)
                print(
                    f"[Iter {iteration}] Sent OpenSky payload (time={payload.get('time')}, "
                    f"states={states_count}) -> {meta.topic}[{meta.partition}]@{meta.offset}"
                )
            except Exception as exc:
                print(f"[Iter {iteration}] Warning: failed to fetch/send OpenSky live states: {exc}")

            if max_iterations is None or iteration < max_iterations:
                time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nStopping live stream producer.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream live OpenSky state vectors to Kafka every 15s.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--iterations", type=int, default=None, help="Optional max iterations (default: run indefinitely)")
    args = parser.parse_args()
    stream_to_kafka(bootstrap_servers=args.bootstrap_servers, max_iterations=args.iterations)

