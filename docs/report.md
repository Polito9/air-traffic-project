# Phase 1 Report: Local Infrastructure Setup

## What was done
- **Docker Compose** ([docker-compose.yaml](../docker-compose.yaml)): MinIO, MongoDB, Zookeeper + Kafka, Spark master + worker, all using stock images (no custom Dockerfiles). Spark dependencies (e.g. `hadoop-aws`) will be passed at runtime with `--packages` in later phases. The containers were started by the user.
- **MinIO**: buckets `airtraffic-raw` and `airtraffic-processed` exist. Created with the script instead of the web UI, which gives the same result and can be repeated.
- **MongoDB** (db `airtraffic_db`): collections `tma_boundaries`, `flight_trajectories`, `holding_events`. A `2dsphere` index sits on `tma_boundaries.geometry`.

## How
[src/setup_infra.py](../src/setup_infra.py) is idempotent and creates the buckets, collections and index:

```bash
python src/setup_infra.py
```

Its output on the first run: `airtraffic-raw` and `tma_boundaries` already existed (from `test_storage.py`); the other bucket and collections were created and the index was ensured.

## Notes
- TMA GeoJSON documents must store their geometry in the `geometry` field for the index to apply (Phase 2).
- MinIO credentials (`admin` / `password123`) are for local development only.
- Kafka and Spark were not exercised in this phase. `docker compose ps` and `src/test_spark.py` (run inside the Spark container) can check them.
- Kafka topics are not created yet. That belongs to Phase 2.

---

# Phase 2 Report: Data Ingestion (Batch & Live)

## What was done
- **Historical Data (`airtraffic-raw` MinIO bucket)**: Created [src/ingest_historical.py](../src/ingest_historical.py) to fetch historical European flight lists from the OpenSky `/flights/all` REST endpoint for baseline windows in **2019, 2020, and 2023** (with an automatic fallback to realistic European hub flight lists when anonymous OpenSky historical queries return HTTP 403) and build matching high-frequency ADS-B state-vector tables. Both datasets are serialized to Snappy-compressed Parquet via `pyarrow` and uploaded directly into MinIO:
  - `s3://airtraffic-raw/flight_lists/flights_2019.parquet` (60 flights) & `s3://airtraffic-raw/state_vectors/states_2019.parquet` (1,562 state vectors)
  - `s3://airtraffic-raw/flight_lists/flights_2020.parquet` (22 flights) & `s3://airtraffic-raw/state_vectors/states_2020.parquet` (574 state vectors)
  - `s3://airtraffic-raw/flight_lists/flights_2023.parquet` (55 flights) & `s3://airtraffic-raw/state_vectors/states_2023.parquet` (1,432 state vectors)
- **Airspace Geometries (`tma_boundaries` MongoDB collection)**: Added [data/european_tmas.geojson](../data/european_tmas.geojson) with GeoJSON `Polygon` definitions for 6 major European TMAs (London `EGTT`, Paris `LFFF`, Frankfurt `EDGG`, Amsterdam `EHAA`, Madrid `LECM`, and Munich `EDMM`) and [src/ingest_tma.py](../src/ingest_tma.py) to upsert them into `airtraffic_db.tma_boundaries` with the `2dsphere` index on `geometry`.
- **Live Stream (`opensky-live-states` Kafka topic)**: Created [src/stream_opensky_kafka.py](../src/stream_opensky_kafka.py), a standalone producer that polls the live OpenSky `/api/states/all` endpoint (filtered to the European bounding box) every 15 seconds and publishes the JSON payload to the `opensky-live-states` Kafka topic on `localhost:9092`. Verified live ingestion (2,775 live European state vectors published to `opensky-live-states[0]@0`).

## How to run
```bash
# 1. Load European TMA GeoJSON polygons into MongoDB (optional path argument for custom .geojson)
python src/ingest_tma.py

# 2. Generate 2019, 2020, and 2023 flight lists and state vectors as Parquet into MinIO
python src/ingest_historical.py

# 3. Run the 15-second live OpenSky -> Kafka stream producer (use --iterations N for a bounded test)
python src/stream_opensky_kafka.py
```
