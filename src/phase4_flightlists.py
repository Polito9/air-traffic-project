"""Phase 4d - Real historical comparison from the OpenSky flight lists (Spark).

Reads s3a://airtraffic-raw/flightlists/flightlist_*.csv.gz (one file per month,
worldwide, one row per flight with origin / destination airport and times) and
counts, for the 6 TMA airports, the daily ARRIVALS and DEPARTURES, plus the
daily arrivals at all airports in Europe (ICAO prefixes E and L).

 * arrivals  : column `day` (UTC day of the last ping = landing day)
 * departures: date of `firstseen`
Only days that belong to the month of the file are kept.

Output: airtraffic_db.airport_daily  (one document per airport/day, plus airport
"EUROPE" with arrivals at every European airport).

Run INSIDE the Spark master container:
  docker compose exec spark-master spark-submit --driver-memory 3g --conf spark.jars.ivy=/tmp/.ivy \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
    /opt/bitnami/spark/src/phase4_flightlists.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
RAW_BUCKET = os.getenv("RAW_BUCKET", "airtraffic-raw")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB = os.getenv("MONGO_DB", "airtraffic_db")
IN_PATH = os.getenv("FLIGHTLIST_PATH", f"s3a://{RAW_BUCKET}/flightlists/flightlist_*.csv.gz")

AIRPORTS = {"EGLL": "London Heathrow", "LFPG": "Paris CDG", "EDDF": "Frankfurt",
            "EHAM": "Amsterdam Schiphol", "LEMD": "Madrid Barajas", "EDDM": "Munich"}


def build_daily(raw):
    """raw: DataFrame with origin, destination, firstseen, day, file. Returns airport/day counts."""
    df = (raw
          .withColumn("file_month", F.regexp_extract("file", r"flightlist_(\d{6})", 1))
          .withColumn("arr_day", F.to_date(F.col("day")))
          .withColumn("dep_day", F.to_date(F.col("firstseen"))))
    in_month = lambda c: F.date_format(F.col(c), "yyyyMM") == F.col("file_month")
    hubs = list(AIRPORTS)
    arr = (df.filter(in_month("arr_day") & F.col("destination").isin(hubs))
           .groupBy(F.col("destination").alias("airport"), F.col("arr_day").alias("date"))
           .agg(F.count("*").alias("arrivals")))
    dep = (df.filter(in_month("dep_day") & F.col("origin").isin(hubs))
           .groupBy(F.col("origin").alias("airport"), F.col("dep_day").alias("date"))
           .agg(F.count("*").alias("departures")))
    eur = (df.filter(in_month("arr_day") & F.col("destination").rlike("^[EL]"))
           .groupBy(F.col("arr_day").alias("date"))
           .agg(F.count("*").alias("arrivals"))
           .withColumn("airport", F.lit("EUROPE")).withColumn("departures", F.lit(0).cast("long")))
    hub = arr.join(dep, ["airport", "date"], "full_outer").na.fill(0, ["arrivals", "departures"])
    out = hub.unionByName(eur.select("airport", "date", "arrivals", "departures"))
    return (out.withColumn("year", F.year("date")).withColumn("month", F.month("date"))
            .withColumn("day_of_month", F.dayofmonth("date"))
            .withColumn("date", F.date_format("date", "yyyy-MM-dd"))
            .withColumn("_id", F.concat_ws(":", "airport", "date"))
            .withColumn("source", F.lit("opensky-flightlist-zenodo")))


def main():
    spark = (SparkSession.builder.appName("phase4d-flightlists")
             .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
             .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
             .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .getOrCreate())
    raw = (spark.read.option("header", True).csv(IN_PATH)
           .select("origin", "destination", "firstseen", "day", F.input_file_name().alias("file")))
    daily = build_daily(raw).cache()
    print(f"[phase4d] {daily.count()} airport/day rows")
    (daily.groupBy("airport", "year", "month")
     .agg(F.sum("arrivals").alias("arrivals"), F.sum("departures").alias("departures"), F.count("*").alias("days"))
     .orderBy("airport", "year").show(60, truncate=False))
    (daily.write.format("mongodb").mode("append")
     .option("connection.uri", MONGO_URI).option("database", MONGO_DB)
     .option("collection", "airport_daily").option("operationType", "replace").save())
    print(f"[phase4d] saved to {MONGO_DB}.airport_daily")
    spark.stop()


if __name__ == "__main__":
    main()
