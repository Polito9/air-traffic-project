# Large-Scale Analysis of European Air Traffic

This repository contains the infrastructure and data processing pipelines for analyzing European ADS-B air traffic data. The project utilizes a containerized architecture to ensure environment consistency across all team members' machines.

## 1. Prerequisites
*   **Docker Desktop**: Must be installed and running.
*   **Python 3.10+**: For local testing and script development.

## 2. Infrastructure Setup (Docker)
The entire data engineering stack (MinIO, MongoDB, Apache Kafka, Zookeeper, and Apache Spark) is containerized. 

To download and start the infrastructure in the background, run the following command in the root directory (where `docker-compose.yml` is located):
```bash
docker compose up -d
