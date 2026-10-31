"""Phase 2b - Upload the real OpenSky flight lists (Zenodo) to MinIO.

Source: "Crowdsourced air traffic data from The OpenSky Network 2020"
https://zenodo.org/records/7923702  (monthly flightlist_YYYYMMDD_YYYYMMDD.csv.gz,
covers 2019-2022). Download the months you need into data/historical/ first.

Uploads every data/historical/flightlist_*.csv.gz to
s3://airtraffic-raw/flightlists/  (same names). Safe to re-run.

Run from the project root with the venv active and Docker up:
    python src/upload_flightlists.py
"""
import glob
import os
import sys

ENDPOINT = os.getenv("MINIO_ENDPOINT_LOCAL", "http://localhost:9000")
ACCESS = os.getenv("MINIO_ACCESS_KEY", "admin")
SECRET = os.getenv("MINIO_SECRET_KEY", "password123")
BUCKET = os.getenv("RAW_BUCKET", "airtraffic-raw")
FOLDER = os.path.join("data", "historical")


def main():
    files = sorted(glob.glob(os.path.join(FOLDER, "flightlist_*.csv.gz")))
    if not files:
        sys.exit(f"No flightlist_*.csv.gz files in {FOLDER}/ - download them from Zenodo first (see README).")
    try:
        import boto3
        from botocore.config import Config
        s3 = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=ACCESS, aws_secret_access_key=SECRET,
                          config=Config(s3={"addressing_style": "path"}), region_name="us-east-1")
        put = lambda path, key: s3.upload_file(path, BUCKET, key)
    except ImportError:
        from minio import Minio
        client = Minio(ENDPOINT.replace("http://", ""), access_key=ACCESS, secret_key=SECRET, secure=False)
        put = lambda path, key: client.fput_object(BUCKET, key, path)
    for f in files:
        key = "flightlists/" + os.path.basename(f)
        print(f"uploading {f} ({os.path.getsize(f) / 1e6:.0f} MB) -> s3://{BUCKET}/{key}")
        put(f, key)
    print("done")


if __name__ == "__main__":
    main()
