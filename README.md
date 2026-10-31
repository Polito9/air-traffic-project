# Large-Scale Analysis of European Air Traffic

This repository contains the infrastructure and data processing pipelines for analyzing European ADS-B air traffic data. The project utilizes a containerized architecture to ensure environment consistency.


## Project Structure

```
├── README.md                  # how to run the project (this file)
├── requirements.txt           # Python dependencies of the ingestion/test scripts
├── docker-compose.yaml        # MinIO, Kafka, Zookeeper, Spark (master + worker), MongoDB
├── data/                      # generated locally, NOT committed
│   ├── parquet/               # simulated 2019/2020/2023 Parquet files (ingest_historical.py)
│   ├── historical/            # real OpenSky flight lists downloaded from Zenodo (section 11)
│   └── european_tmas_simplified.geojson   # 6 simplified TMA polygons (load_tma_polygons.py)
├── src/
│   ├── setup_infra.py                 # Phase 1 – create MinIO buckets, MongoDB collections and 2dsphere index
│   ├── test_storage.py                # Phase 1 – check MinIO and MongoDB connections
│   ├── test_spark.py                  # Phase 1 – check Spark inside the container
│   ├── ingest_tma.py                  # Phase 2 – load TMA polygons from a GeoJSON file into MongoDB
│   ├── load_tma_polygons.py           # Phase 2 – fallback: generate and load 6 simplified TMA polygons
│   ├── ingest_historical.py           # Phase 2 – historical files (OpenSky if available, otherwise simulated)
│   ├── stream_opensky_kafka.py        # Phase 2 – live OpenSky -> Kafka producer (basic auth)
│   ├── stream_opensky_kafka_oauth.py  # Phase 2 – live OpenSky -> Kafka producer (OAuth2, gzip, 429 handling)
│   ├── phase3_kafka_to_minio.py       # Phase 3a – Spark: Kafka topic -> normalized Parquet in MinIO
│   ├── phase3_clean_sessionize.py     # Phase 3b – Spark: cleaning, sessionization, GeoJSON trajectories -> MongoDB
│   ├── check_phase3.py                # Phase 3 – print trajectory counts from MongoDB
│   ├── phase4_holdings.py             # Phase 4a – Spark: holding pattern detection -> holding_events
│   ├── phase4_congestion.py           # Phase 4b – MongoDB $geoIntersects congestion per TMA -> tma_congestion
│   ├── phase4_hourly.py               # Phase 4c – traffic and holdings by time of day -> hourly_traffic
│   ├── upload_flightlists.py          # Phase 4d – upload the real OpenSky flight lists (Zenodo) to MinIO
│   ├── phase4_flightlists.py          # Phase 4d – Spark: daily arrivals/departures per airport -> airport_daily
│   ├── report_stats.py                # prints every figure quoted in the final report
│   └── dashboard.py                   # Phase 5 – Streamlit + Kepler.gl dashboard (runs locally, not in Docker)
└── docs/
    └── final_report.md        # final 2-page report (objective, dataset, methodology, results, deviations)
```

MinIO buckets: `airtraffic-raw` (raw and landed data) and `airtraffic-processed` (cleaned points).
MongoDB database `airtraffic_db`: `tma_boundaries`, `flight_trajectories`, `holding_events`, `tma_congestion`, `hourly_traffic`, `airport_daily`.

---

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

   > [!NOTE]
   > `data/european_tmas.geojson` is not in the repository. If you do not have it, run
   > `python src/load_tma_polygons.py` instead: it loads 6 simplified TMA areas
   > (60 km circles around London, Paris, Frankfurt, Amsterdam, Madrid and Munich).

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

---

## 7. Running Phase 3: Data Cleaning & Flight Grouping

Phase 3 runs inside the Spark container. We use `docker compose exec spark-master`, which works whatever the project folder is called. The first run of each job downloads its Spark packages (a few minutes). Use `--driver-memory 3g` when the live capture is large (millions of rows).

**7.1 Land the live stream (Kafka -> MinIO).** Reads the whole `opensky-live-states` topic, turns each OpenSky payload into one row per aircraft state, removes repeated states and writes Parquet to `s3://airtraffic-raw/state_vectors/live/` (re-running overwrites it):
```bash
docker compose exec spark-master spark-submit --driver-memory 3g --conf spark.jars.ivy=/tmp/.ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/bitnami/spark/src/phase3_kafka_to_minio.py
```

**7.2 Clean, sessionize and store trajectories.** Drops rows with missing coordinates or speed above Mach 1, starts a new flight session after a gap of more than 30 minutes per aircraft, and saves one GeoJSON `LineString` per session in MongoDB `airtraffic_db.flight_trajectories` (live data are labelled `dataset=live`, the simulated ones `synthetic_<year>`):
```bash
docker compose exec spark-master spark-submit --driver-memory 3g --conf spark.jars.ivy=/tmp/.ivy --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 /opt/bitnami/spark/src/phase3_clean_sessionize.py
```
Check the result with `python src/check_phase3.py`.

> [!IMPORTANT]
> Before re-running 7.1/7.2 on a **new live capture**, delete the old live results so trajectories are not duplicated:
> ```bash
> docker compose exec mongodb mongosh airtraffic_db --eval "db.flight_trajectories.deleteMany({dataset:'live'}); db.holding_events.deleteMany({})"
> ```

---

## 8. Running Phase 4: Analytics

1. **Holding patterns (Spark).** A window over each flight session sums the signed heading change over 20 minutes; at least 360 degrees inside a ~50 km box (speed at least 75 m/s, 6 or more pings, 2 or more minutes) is a holding. Results go to `holding_events`:
   ```bash
   docker compose exec spark-master spark-submit --driver-memory 3g --conf spark.jars.ivy=/tmp/.ivy --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 /opt/bitnami/spark/src/phase4_holdings.py
   ```
2. **Congestion per TMA (MongoDB `$geoIntersects`).** Counts the trajectories crossing each TMA and the distinct aircraft inside it per 10-minute bucket (`tma_congestion`):
   ```bash
   python src/phase4_congestion.py
   ```
3. **Traffic by time of day.** Average aircraft seen per 10 minutes and holdings per hour, UTC (`hourly_traffic`):
   ```bash
   python src/phase4_hourly.py
   ```
4. **Figures used in the report:**
   ```bash
   python src/report_stats.py
   ```

> [!NOTE]
> Diversions (final position versus planned destination) are **not implemented**: live state vectors contain no planned destination and the simulated flights never divert. See `docs/final_report.md`.

---

## 9. Running Phase 5: Dashboard

The dashboard runs locally in the virtual environment (not in Docker) and reads MongoDB on `localhost:27017`:
```bash
pip install streamlit keplergl pydeck pymongo pandas
python -m streamlit run src/dashboard.py
```
Open http://localhost:8501. Tabs: **Map** (trajectories, TMA areas and holdings; pick "Basemap (pydeck)" or "Kepler.gl" in the sidebar), **TMA congestion**, **Traffic trends** (time of day on real data; simulated years in a separate expander) and **Holding events**.

If `streamlit` is blocked by Windows ("Control de aplicaciones"), `python -m streamlit` avoids it.

---

## 10. Capturing live data with an OpenSky API client (OAuth2)

OpenSky no longer accepts user/password. Create an API client in your OpenSky account (https://opensky-network.org/my-opensky/account) and download its `credentials.json` (it holds `clientId` and `clientSecret`). **Never commit it to the repository.** A standard account has 4,000 credits per day; a request over all of Europe costs 4 credits, so about 1,000 requests per day (anonymous access: 400 credits).

Load the credentials into the terminal and start the stream (PowerShell):
```powershell
$c = Get-Content "$env:USERPROFILE\Downloads\credentials.json" | ConvertFrom-Json
$env:OPENSKY_CLIENT_ID = $c.clientId; $env:OPENSKY_CLIENT_SECRET = $c.clientSecret
python src/stream_opensky_kafka_oauth.py --interval 30 --iterations 500
```
macOS / Linux:
```bash
export OPENSKY_CLIENT_ID=... OPENSKY_CLIENT_SECRET=...
python src/stream_opensky_kafka_oauth.py --interval 30 --iterations 500
```
The script waits automatically when the quota is exhausted (HTTP 429) and prints `credits left=...` on every request. Options: `--bbox LAMIN LOMIN LAMAX LOMAX`, `--bootstrap-servers`, `--iterations`.

---

## 11. Real historical comparison (COVID-19: Feb-Jun 2019/2020 and April 2022)

The OpenSky flight lists are public (Zenodo record 7923702, one `.csv.gz` per month, 2019-2022). They contain origin, destination and times of every flight, but no trajectories.

1. Download the eleven files into `data/historical/` (do not unzip them): February to June of 2019 and 2020, and April 2022 (about 1.6 GB):
   ```powershell
   New-Item -ItemType Directory -Force data\historical | Out-Null
   $base = "https://zenodo.org/records/7923702/files"
   $files = "flightlist_20190201_20190228","flightlist_20190301_20190331","flightlist_20190401_20190430","flightlist_20190501_20190531","flightlist_20190601_20190630","flightlist_20200201_20200229","flightlist_20200301_20200331","flightlist_20200401_20200430","flightlist_20200501_20200531","flightlist_20200601_20200630","flightlist_20220401_20220430"
   foreach ($f in $files) { Invoke-WebRequest "$base/$f.csv.gz`?download=1" -OutFile "data\historical\$f.csv.gz" }
   ```
2. Upload them to MinIO (venv active, Docker up): `python src/upload_flightlists.py`
3. Count daily arrivals and departures with Spark (writes `airtraffic_db.airport_daily`):
   ```bash
   docker compose exec spark-master spark-submit --driver-memory 3g --conf spark.jars.ivy=/tmp/.ivy --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 /opt/bitnami/spark/src/phase4_flightlists.py
   ```
4. The dashboard (tab "Traffic trends") shows the comparison.

## 12. Full run, in order

```text
1. docker compose up -d
2. python src/setup_infra.py
3. python src/ingest_historical.py            # simulated 2019/2020/2023 data
4. python src/load_tma_polygons.py            # or src/ingest_tma.py with a GeoJSON file
5. python src/stream_opensky_kafka_oauth.py --interval 30 --iterations 500   # live data (needs credentials)
6. spark-submit phase3_kafka_to_minio.py, then phase3_clean_sessionize.py     # section 7
7. spark-submit phase4_holdings.py                                            # section 8
8. python src/phase4_congestion.py ; python src/phase4_hourly.py ; python src/report_stats.py
9. download flight lists, python src/upload_flightlists.py, spark-submit phase4_flightlists.py   # section 11
10. python -m streamlit run src/dashboard.py
```
Steps 1-4 and 6-10 also work with only the simulated data (without step 5), but then the results only validate the pipeline.
