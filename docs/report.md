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

