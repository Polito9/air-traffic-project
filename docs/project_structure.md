# Large-Scale Analysis of European Air Traffic

## 1. Project Overview
This project provides a robust framework for processing, analyzing, and visualizing vast volumes of European ADS-B air traffic data. By combining batch and real-time streaming architectures, the pipeline enables continuous monitoring and retrospective spatial analysis of European airspace.
*   **Reconstructing Trajectories:** Stitch raw, high-frequency ADS-B state vectors into clean, continuous flight trajectories across variable update intervals.
*   **Detecting Holding Patterns:** Identify airborne delay loops automatically using geometric heuristics and trajectory curvature analysis in near-terminal airspace.
*   **Quantifying Terminal Congestion:** Measure traffic density, spatial overlap, and delay metrics inside Terminal Maneuvering Areas (TMAs) using geospatial index querying.
*   **Macro-Level Comparison:** Compare pre- and post-disruption traffic flows, route distribution changes, and regional sector loads across distinct operational seasons.

---

## 2. Required Technologies & Installation Guide
To maintain environment consistency across engineering teams, containerized deployment via Docker Compose is strictly recommended.

### Technology Stack
*   **Storage Layer:** MinIO (S3-compatible Object Storage) for raw historical Parquet storage.
*   **Streaming Ingestion:** Apache Kafka for buffering live state-vector streams from the OpenSky Network API.
*   **Processing Engine:** Apache Spark / PySpark for large-scale distributed sessionization, geometric extraction, and spatial calculations.
*   **Database Layer:** MongoDB with 2D/2DSphere indexing for storing spatial GeoJSON geometries and structured trajectory metadata.
*   **Visualization Layer:** Streamlit integrated with Kepler.gl for high-performance visual rendering of complex spatial trajectories.

### Service Orchestration (Final Docker Configuration)
Deploy the unified infrastructure stack using `docker-compose up -d`. Below are the finalized, working images and their respective ports:

| Service | Image | Default Port | Primary Function |
| :--- | :--- | :--- | :--- |
| **MinIO** | `cgr.dev/chainguard/minio:latest` | 9000 / 9001 | Raw Parquet landing zone and cold storage |
| **Zookeeper** | `confluentinc/cp-zookeeper:7.5.0` | 2181 | Required for Kafka coordination |
| **Apache Kafka** | `confluentinc/cp-kafka:7.5.0` | 9092 | Real-time ADS-B message queuing |
| **Apache Spark** | `bitnamilegacy/spark:3.5.0` | 7077 / 8080 / 8081 | Distributed batch and stream processing |
| **MongoDB** | `mongo:7.0` | 27017 | Geospatial document indexing |
| **Streamlit** | `Custom Dockerfile` | 8501 | Analytical dashboard server |

---

## 3. Project Phases
### Phase 1: Local Infrastructure Setup
**Objective:** Get the core databases and storage running locally without complex cloud partitioning or custom Dockerfiles.
- **Docker Compose:** Spin up MinIO, MongoDB, Kafka, and Spark. Use the `--packages` flag at runtime for Spark dependencies (e.g., `hadoop-aws`) instead of building custom images.
- **MinIO:** Create two basic buckets via the web interface: `airtraffic-raw` (for raw downloads) and `airtraffic-processed` (for cleaned outputs). 
- **MongoDB:** Create standard collections (`tma_boundaries`, `flight_trajectories`, `holding_events`) and apply a basic `2dsphere` index to the boundary geometries.

### Phase 2: Data Ingestion (Batch & Live)
**Objective:** Load data into the system without over-engineering dual-sinks or micro-batch routing architectures.
- **Historical Data:** Download via API the OpenSky Parquet files (state vectors and flight lists for 2019, 2020, 2023) and upload them directly into the `airtraffic-raw` MinIO bucket.
- **Airspace Geometries:** Load standard GeoJSON polygons for major European TMAs (Terminal Maneuvering Areas) into MongoDB.
- **Live Stream:** Write a standalone Python script that requests data from the OpenSky REST API every 15 seconds and sends the JSON payload to the Kafka topic.

### Phase 3: Data Cleaning & Flight Grouping
**Objective:** Turn noisy, raw pings into continuous flight paths.
- **Noise Filtering:** Write a PySpark job to drop rows with missing coordinates or physically impossible speeds (e.g., > Mach 1).
- **Sessionization:** Group the pings by aircraft ID (`icao24`). Split the data into separate flight sessions if there is a time gap of more than 30 minutes between consecutive pings.
- **Storage:** Save the grouped coordinates as continuous GeoJSON `LineString` paths into MongoDB.

### Phase 4: Analytics (Holding Patterns & Congestion)
**Objective:** Identify delays and calculate traffic density using straightforward queries.
- **Holding Patterns:** Use a PySpark window function to track heading changes. If an aircraft turns more than 360 degrees within a small geographic cluster, flag it as a holding pattern and save the event to MongoDB.
- **Diversions:** Compare the flight's final coordinate against its planned destination from the Flight Lists to flag diverted flights.
- **Congestion:** Use basic MongoDB `$geoIntersects` queries to count how many active flights overlap with a TMA polygon during specific hours.

### Phase 5: Dashboard Visualization
**Objective:** Visualize the findings in a simple web app running locally.
- **Streamlit & Kepler.gl:** Build a local Streamlit app (run via standard Python in your virtual environment, not Docker) that pulls data directly from MongoDB.
- **Map View:** Render the flight paths, TMA boundaries, and holding patterns on a Kepler.gl interactive map.
- **Trend Charts:** Create simple bar and line charts comparing total flight volumes and fleet utilization across the 2019, 2020, and 2023 baseline years.
