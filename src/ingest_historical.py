"""Phase 2: Fetch or generate historical OpenSky flight lists and state vectors (2019, 2020, 2023)
as Parquet files and upload them to the 'airtraffic-raw' MinIO bucket.
"""
import io
import math
import os
from pathlib import Path
import random
from datetime import datetime, timezone
from minio import Minio
import pyarrow as pa
import pyarrow.parquet as pq
import requests

BUCKET_NAME = "airtraffic-raw"
LOCAL_PARQUET_DIR = Path(__file__).resolve().parent.parent / "data" / "parquet"
YEARS = [2019, 2020, 2023]

# Major European hub airports (ICAO, lat, lon)
EU_AIRPORTS = [
    ("EGLL", 51.4700, -0.4543),   # London Heathrow
    ("LFPG", 49.0097, 2.5479),    # Paris CDG
    ("EDDF", 50.0379, 8.5622),    # Frankfurt
    ("EHAM", 52.3105, 4.7683),    # Amsterdam Schiphol
    ("LEMD", 40.4983, -3.5676),   # Madrid Barajas
    ("EDDM", 48.3537, 11.7750),   # Munich
]

# Sample 1-hour windows (June 15, 10:00-11:00 UTC) for each baseline year
SAMPLE_WINDOWS = {
    2019: (
        int(datetime(2019, 6, 15, 10, 0, tzinfo=timezone.utc).timestamp()),
        int(datetime(2019, 6, 15, 11, 0, tzinfo=timezone.utc).timestamp()),
    ),
    2020: (
        int(datetime(2020, 6, 15, 10, 0, tzinfo=timezone.utc).timestamp()),
        int(datetime(2020, 6, 15, 11, 0, tzinfo=timezone.utc).timestamp()),
    ),
    2023: (
        int(datetime(2023, 6, 15, 10, 0, tzinfo=timezone.utc).timestamp()),
        int(datetime(2023, 6, 15, 11, 0, tzinfo=timezone.utc).timestamp()),
    ),
}

# Volume baseline per year (reflecting pre-COVID 2019, COVID drop in 2020, recovery in 2023)
FLIGHT_COUNTS = {2019: 60, 2020: 22, 2023: 55}


def fetch_opensky_flights(year: int) -> list[dict]:
    """Try fetching historical flights from OpenSky REST API (/flights/all)."""
    begin, end = SAMPLE_WINDOWS[year]
    url = f"https://opensky-network.org/api/flights/all?begin={begin}&end={end}"
    auth = None
    user = os.getenv("OPENSKY_USERNAME")
    pw = os.getenv("OPENSKY_PASSWORD")
    if user and pw:
        auth = (user, pw)

    try:
        resp = requests.get(url, auth=auth, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            eu_codes = {code for code, _, _ in EU_AIRPORTS}
            filtered = [
                f for f in data
                if f.get("estDepartureAirport") in eu_codes or f.get("estArrivalAirport") in eu_codes
            ]
            flights = filtered[:FLIGHT_COUNTS[year]] if filtered else data[:FLIGHT_COUNTS[year]]
            if flights:
                print(f"[{year}] Fetched {len(flights)} flights from OpenSky REST API.")
                return [
                    {
                        "icao24": str(f.get("icao24") or f"{random.randint(0x300000, 0x4fffff):06x}"),
                        "firstseen": int(f.get("firstSeen") or begin),
                        "estdepartureairport": str(f.get("estDepartureAirport") or "EGLL"),
                        "lastseen": int(f.get("lastSeen") or end),
                        "estarrivalairport": str(f.get("estArrivalAirport") or "LFPG"),
                        "callsign": str(f.get("callsign") or "EZY101").strip(),
                        "day": int(begin - (begin % 86400)),
                    }
                    for f in flights
                ]
        else:
            print(f"[{year}] OpenSky /flights/all returned HTTP {resp.status_code}; using realistic European baseline fallback.")
    except Exception as exc:
        print(f"[{year}] OpenSky API unreachable ({exc}); using realistic European baseline fallback.")

    return build_fallback_flights(year)


def build_fallback_flights(year: int) -> list[dict]:
    """Build deterministic European flight list records for the given year."""
    rng = random.Random(year)
    begin, end = SAMPLE_WINDOWS[year]
    count = FLIGHT_COUNTS[year]
    airlines = ["BAW", "AFR", "DLH", "KLM", "IBE", "EZY", "RYR"]
    flights = []

    for idx in range(count):
        dep, arr = rng.sample(EU_AIRPORTS, 2)
        first_seen = begin + rng.randint(0, 900)
        last_seen = first_seen + rng.randint(1800, 3300)
        flights.append({
            "icao24": f"{0x3c0000 + year * 100 + idx:06x}",
            "firstseen": first_seen,
            "estdepartureairport": dep[0],
            "lastseen": min(last_seen, end + 1800),
            "estarrivalairport": arr[0],
            "callsign": f"{rng.choice(airlines)}{100 + idx}",
            "day": begin - (begin % 86400),
        })
    return flights


def build_state_vectors_for_flights(year: int, flights: list[dict]) -> list[dict]:
    """Generate OpenSky state-vector pings along each flight trajectory (including holding patterns and edge-case noise for Phase 3/4)."""
    rng = random.Random(year + 99)
    airport_coords = {code: (lat, lon) for code, lat, lon in EU_AIRPORTS}
    vectors = []

    for i, fl in enumerate(flights):
        dep_lat, dep_lon = airport_coords.get(fl["estdepartureairport"], EU_AIRPORTS[0][1:])
        arr_lat, arr_lon = airport_coords.get(fl["estarrivalairport"], EU_AIRPORTS[1][1:])
        t0 = fl["firstseen"]
        t1 = max(fl["lastseen"], t0 + 1200)
        steps = 25
        dt = max(15, (t1 - t0) // steps)

        # Every 6th flight performs a holding pattern loop near arrival TMA
        has_holding = (i % 6 == 0)

        for s in range(steps + 1):
            frac = s / steps
            t = t0 + s * dt
            lat = dep_lat + (arr_lat - dep_lat) * frac
            lon = dep_lon + (arr_lon - dep_lon) * frac

            # Calculate base heading
            dlat = arr_lat - dep_lat
            dlon = arr_lon - dep_lon
            heading = (math.degrees(math.atan2(dlon, dlat)) + 360.0) % 360.0

            if has_holding and s >= steps - 8:
                # 2 full circular loops near arrival airport
                angle_deg = (s - (steps - 8)) * 90.0
                rad = math.radians(angle_deg)
                lat = arr_lat + 0.08 * math.cos(rad)
                lon = arr_lon + 0.08 * math.sin(rad)
                heading = (angle_deg + 90.0) % 360.0

            vectors.append({
                "time": int(t),
                "icao24": fl["icao24"],
                "lat": float(round(lat, 6)),
                "lon": float(round(lon, 6)),
                "velocity": float(round(rng.uniform(180.0, 250.0), 2)),
                "heading": float(round(heading, 2)),
                "vertrate": float(round(rng.uniform(-5.0, 5.0), 2)),
                "callsign": fl["callsign"],
                "onground": bool(s == 0 or s == steps),
                "alert": False,
                "spi": False,
                "squawk": "1000",
                "baroaltitude": float(round(10000.0 * math.sin(math.pi * frac), 1)),
                "geoaltitude": float(round(10150.0 * math.sin(math.pi * frac), 1)),
                "lastposupdate": float(t),
                "lastcontact": float(t),
                "hour": int(t - (t % 3600)),
            })

    # Add a couple of noisy pings (missing coords or > Mach 1 velocity) so Phase 3 filtering has real noise to drop
    vectors.append({
        "time": int(SAMPLE_WINDOWS[year][0]),
        "icao24": "ffffff",
        "lat": None,
        "lon": None,
        "velocity": 210.0,
        "heading": 0.0,
        "vertrate": 0.0,
        "callsign": "NOISE1",
        "onground": False,
        "alert": False,
        "spi": False,
        "squawk": "0000",
        "baroaltitude": 5000.0,
        "geoaltitude": 5000.0,
        "lastposupdate": float(SAMPLE_WINDOWS[year][0]),
        "lastcontact": float(SAMPLE_WINDOWS[year][0]),
        "hour": int(SAMPLE_WINDOWS[year][0]),
    })
    vectors.append({
        "time": int(SAMPLE_WINDOWS[year][0] + 15),
        "icao24": "fffffe",
        "lat": 50.0,
        "lon": 8.0,
        "velocity": 500.0,  # > Mach 1 (343 m/s)
        "heading": 90.0,
        "vertrate": 0.0,
        "callsign": "NOISE2",
        "onground": False,
        "alert": False,
        "spi": False,
        "squawk": "0000",
        "baroaltitude": 9000.0,
        "geoaltitude": 9000.0,
        "lastposupdate": float(SAMPLE_WINDOWS[year][0] + 15),
        "lastcontact": float(SAMPLE_WINDOWS[year][0] + 15),
        "hour": int(SAMPLE_WINDOWS[year][0]),
    })

    return vectors


def upload_parquet_to_minio(client: Minio, object_name: str, rows: list[dict]):
    table = pa.Table.from_pylist(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    data_bytes = buf.getvalue()

    # Also save locally under data/parquet/ for easy inspection
    local_path = LOCAL_PARQUET_DIR / object_name
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_bytes(data_bytes)

    client.put_object(
        BUCKET_NAME,
        object_name,
        io.BytesIO(data_bytes),
        length=len(data_bytes),
        content_type="application/octet-stream",
    )
    print(f"Saved {local_path.relative_to(LOCAL_PARQUET_DIR.parent.parent)} & uploaded s3://{BUCKET_NAME}/{object_name} ({len(rows)} rows, {len(data_bytes)} bytes)")


def main():
    minio_client = Minio("localhost:9000", access_key="admin", secret_key="password123", secure=False)
    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)

    for year in YEARS:
        flights = fetch_opensky_flights(year)
        states = build_state_vectors_for_flights(year, flights)

        upload_parquet_to_minio(minio_client, f"flight_lists/flights_{year}.parquet", flights)
        upload_parquet_to_minio(minio_client, f"state_vectors/states_{year}.parquet", states)


if __name__ == "__main__":
    main()

