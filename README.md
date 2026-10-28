# Large-Scale Analysis of European Air Traffic

This repository contains the infrastructure and data processing pipelines for analyzing European ADS-B air traffic data. The project utilizes a containerized architecture to ensure environment consistency.

## 1. Prerequisites
*   **Docker Desktop**: Must be installed and running.
*   **Python 3.10+**: For local testing and script development.

## 2. Infrastructure Setup (Docker)
The entire data engineering stack (MinIO, MongoDB, Apache Kafka, Zookeeper, and Apache Spark) is containerized. 

To download and start the infrastructure in the background, run the following command in the root directory (where `docker-compose.yml` is located):
```bash
docker compose up -d
```

> [!NOTE]
> The initial download will take a few minutes. Subsequent startups will be nearly instantaneous.


---

## 3. Python Environment & Requirements
To interact with the infrastructure locally (e.g., testing the MinIO and MongoDB connections), you need to set up a virtual environment and install the required Python packages.

### Create and activate a virtual environment

**Windows (PowerShell / Command Prompt):**
```bash
python -m venv venv
venv\Scripts\activate
```

**macOS / Linux:**
```bash
python -m venv venv
source venv/bin/activate
```

### Install dependencies
```bash
pip install -r requirements.txt
```

---

## 4. Testing the Environment
Once the Docker containers are up and your virtual environment is active, you can verify that the storage and database layers are accepting connections.

Run the test script from your local terminal:
```bash
python src/test_storage.py
```

If successful, you will see `SUCCESS` messages for both MinIO and MongoDB, and the `airtraffic-raw` bucket will be created.

---

## 5. Project Workflow & Running Setup / Data Ingestion

### End-to-End Pipeline Workflow
1. **Phase 1 — Local Infrastructure Setup:** Initialize MinIO buckets (`airtraffic-raw`, `airtraffic-processed`) and MongoDB collections (`tma_boundaries`, `flight_trajectories`, `holding_events`) with `2dsphere` spatial indexing.
2. **Phase 2 — Data Ingestion (Batch & Live):**
   * Load European Terminal Maneuvering Area (TMA) GeoJSON polygons into MongoDB.
   * Fetch/generate 2019, 2020, and 2023 flight lists and state-vector trajectories as Parquet files (saved locally in `data/parquet/` and uploaded to MinIO `airtraffic-raw`).
   * Stream live European state vectors from the OpenSky REST API into Apache Kafka (`opensky-live-states` topic) every 15 seconds.
3. **Phase 3 — Data Cleaning & Flight Grouping:** Filter noisy pings and sessionize aircraft state vectors into continuous GeoJSON `LineString` trajectories using PySpark.
4. **Phase 4 — Spatial Analytics:** Detect holding patterns (cumulative turns > 360°), diversions, and TMA congestion (`$geoIntersects`).
5. **Phase 5 — Visualization:** Render trajectories, TMA polygons, holding events, and multi-year trends in Streamlit + Kepler.gl.

### Running Phase 1: Infrastructure Initialization
With the Docker containers running and your virtual environment activated, initialize the buckets, collections, and geospatial index:
```bash
python src/setup_infra.py
```

### Running Phase 2: Data Ingestion
1. **Load European TMA Boundaries into MongoDB:**
   ```bash
   python src/ingest_tma.py
   ```
   *(Optional: pass a custom `.geojson` file path as an argument: `python src/ingest_tma.py path/to/custom.geojson`)*

2. **Ingest Historical Parquet Datasets (2019, 2020, 2023) into MinIO:**
   ```bash
   python src/ingest_historical.py
   ```
   This saves the `flight_lists` and `state_vectors` Parquet files locally under `data/parquet/` and uploads them to `s3://airtraffic-raw/`.

3. **Start the Live OpenSky -> Kafka Stream Producer:**
   ```bash
   # Run continuously (polls OpenSky every 15 seconds)
   python src/stream_opensky_kafka.py

   # Or run a single test iteration
   python src/stream_opensky_kafka.py --iterations 1
   ```

---

## 6. Executing Spark Queries

### When to use Spark
Use PySpark for the heavy, distributed data processing tasks outlined in the project phases. This includes:
* **Batch cleaning** of historical ADS-B Parquet files.
* **Trajectory sessionization** (grouping state vectors by transponder ID).
* **Advanced spatial analytics** (calculating heading changes and detecting holding patterns).

### How to execute Spark scripts

> [!IMPORTANT]
> **Do not run PySpark scripts directly from your local terminal** (e.g., `python src/my_spark_job.py`). Doing so can cause Python version mismatches (leading to serialization StackOverflow errors) and trigger Windows temporary file locking bugs.

Instead, execute the scripts inside the Spark Master container, where the environment is perfectly synced. The `src` folder is automatically mounted into the container at `/opt/bitnami/spark/src`.

To submit a Spark job, use the following Docker command:
```bash
docker exec -it air-traffic-project-spark-master-1 spark-submit /opt/bitnami/spark/src/<script_name>.py
```
