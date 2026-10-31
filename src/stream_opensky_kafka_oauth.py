"""Live OpenSky -> Kafka producer with OAuth2 (NEW file; the original
stream_opensky_kafka.py is left untouched).

Differences with the original:
  * OAuth2 client-credentials login (OpenSky no longer accepts user/password).
    Set OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET. Without them it runs
    anonymously (much smaller daily quota).
  * Configurable polling interval and bounding box (--interval, --bbox).
  * Waits and retries on HTTP 429 (quota exhausted) instead of hammering the API.
  * Prints the remaining credits when OpenSky returns that header.
  * gzip compression + larger max request size, so big payloads are not rejected
    by Kafka's default 1 MB limit.

Usage (project root, venv active):
    $env:OPENSKY_CLIENT_ID="..."; $env:OPENSKY_CLIENT_SECRET="..."      # PowerShell
    python src/stream_opensky_kafka_oauth.py --interval 30
"""
import argparse
import json
import os
import time

import requests
from kafka import KafkaProducer

STATES_URL = "https://opensky-network.org/api/states/all"
TOKEN_URL = ("https://auth.opensky-network.org/auth/realms/opensky-network/"
             "protocol/openid-connect/token")
TOPIC = "opensky-live-states"
DEFAULT_BBOX = (35.0, -12.0, 62.0, 30.0)   # lamin, lomin, lamax, lomax (Europe)

_tok = {"value": None, "exp": 0.0}


def get_token():
    cid = os.getenv("OPENSKY_CLIENT_ID")
    secret = os.getenv("OPENSKY_CLIENT_SECRET")
    if not cid or not secret:
        return None
    if _tok["value"] is None or time.time() >= _tok["exp"]:
        r = requests.post(TOKEN_URL, data={
            "grant_type": "client_credentials",
            "client_id": cid, "client_secret": secret}, timeout=30)
        r.raise_for_status()
        d = r.json()
        _tok["value"] = d["access_token"]
        _tok["exp"] = time.time() + int(d.get("expires_in", 1800)) - 60  # refresh early
    return _tok["value"]


def fetch_states(bbox):
    lamin, lomin, lamax, lomax = bbox
    params = {"lamin": lamin, "lomin": lomin, "lamax": lamax, "lomax": lomax}
    headers = {}
    token = get_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    resp = requests.get(STATES_URL, params=params, headers=headers, timeout=20)
    if resp.status_code == 401:          # token expired/invalid -> force refresh next time
        _tok["value"] = None
    return resp


def main():
    ap = argparse.ArgumentParser(description="Stream live OpenSky states to Kafka.")
    ap.add_argument("--bootstrap-servers", default="localhost:9092")
    ap.add_argument("--interval", type=float, default=30.0, help="seconds between polls")
    ap.add_argument("--bbox", type=float, nargs=4, default=DEFAULT_BBOX,
                    metavar=("LAMIN", "LOMIN", "LAMAX", "LOMAX"))
    ap.add_argument("--iterations", type=int, default=None)
    args = ap.parse_args()

    if not (os.getenv("OPENSKY_CLIENT_ID") and os.getenv("OPENSKY_CLIENT_SECRET")):
        print("WARNING: no OPENSKY_CLIENT_ID/SECRET set -> anonymous access (small quota).")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        compression_type="gzip",
        max_request_size=10 * 1024 * 1024,
    )
    print(f"Streaming to '{TOPIC}' every {args.interval}s, bbox={tuple(args.bbox)}")

    i = 0
    try:
        while args.iterations is None or i < args.iterations:
            i += 1
            wait = args.interval
            try:
                r = fetch_states(args.bbox)
                if r.status_code == 429:
                    wait = max(wait, float(r.headers.get("X-Rate-Limit-Retry-After-Seconds", 60)))
                    print(f"[{i}] Quota exhausted (429). Waiting {wait:.0f}s.")
                else:
                    r.raise_for_status()
                    payload = r.json()
                    meta = producer.send(TOPIC, value=payload).get(timeout=20)
                    n = len(payload.get("states") or [])
                    left = r.headers.get("X-Rate-Limit-Remaining")
                    extra = f", credits left={left}" if left is not None else ""
                    print(f"[{i}] time={payload.get('time')} states={n} -> "
                          f"{meta.topic}[{meta.partition}]@{meta.offset}{extra}")
            except Exception as exc:
                print(f"[{i}] Warning: {type(exc).__name__}: {exc}")
            if args.iterations is None or i < args.iterations:
                time.sleep(wait)
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
