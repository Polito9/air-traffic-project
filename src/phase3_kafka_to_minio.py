"""Phase 3a: Land the live OpenSky stream (Kafka) as Parquet in MinIO.

Reads everything currently stored in the Kafka topic `opensky-live-states`
(batch read, so it is safe to re-run: the output is overwritten with the same
content plus whatever arrived since), explodes each OpenSky payload into one
row per aircraft state and normalises it to the common schema used by the
historical files:

    icao24, callsign, time, lat, lon, velocity, heading, vertrate,
    baroaltitude, geoaltitude, onground, source, dataset

Output: s3a://airtraffic-raw/state_vectors/live/  (Parquet, Snappy)

Run INSIDE the Spark master container (see README), e.g.:
    spark-submit --conf spark.jars.ivy=/tmp/.ivy \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
      /opt/bitnami/spark/src/phase3_kafka_to_minio.py

Service host names / ports can be overridden with environment variables
(KAFKA_BOOTSTRAP, MINIO_ENDPOINT, ...) because inside Docker they are NOT
`localhost`.
"""
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "opensky-live-states")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
RAW_BUCKET = os.getenv("RAW_BUCKET", "airtraffic-raw")
OUT_PATH = f"s3a://{RAW_BUCKET}/state_vectors/live/"

# OpenSky /states/all payload: {"time": <epoch>, "states": [[...17 fields...], ...]}
# Mixed types inside each state (str/number/bool/null), so read every element
# as string and cast afterwards.
PAYLOAD_SCHEMA = T.StructType([
    T.StructField("time", T.LongType()),
    T.StructField("states", T.ArrayType(T.ArrayType(T.StringType()))),
])

COMMON_COLS = ["icao24", "callsign", "time", "lat", "lon", "velocity", "heading",
               "vertrate", "baroaltitude", "geoaltitude", "onground",
               "source", "dataset"]


def normalize_live(raw_json_df: DataFrame) -> DataFrame:
    """raw_json_df has one column `json` (the Kafka message value as string).

    OpenSky state vector indexes: 0 icao24, 1 callsign, 2 origin_country,
    3 time_position, 4 last_contact, 5 longitude, 6 latitude, 7 baro_altitude,
    8 on_ground, 9 velocity (m/s), 10 true_track (deg), 11 vertical_rate,
    12 sensors, 13 geo_altitude, 14 squawk, 15 spi, 16 position_source.
    """
    parsed = (raw_json_df
              .select(F.from_json("json", PAYLOAD_SCHEMA).alias("p"))
              .select(F.explode("p.states").alias("s")))  # null/empty states -> no rows

    norm = parsed.select(
        F.col("s")[0].alias("icao24"),
        F.trim(F.col("s")[1]).alias("callsign"),
        # position timestamp (falls back to last contact when missing)
        F.coalesce(F.col("s")[3], F.col("s")[4]).cast("long").alias("time"),
        F.col("s")[6].cast("double").alias("lat"),
        F.col("s")[5].cast("double").alias("lon"),
        F.col("s")[9].cast("double").alias("velocity"),
        F.col("s")[10].cast("double").alias("heading"),
        F.col("s")[11].cast("double").alias("vertrate"),
        F.col("s")[7].cast("double").alias("baroaltitude"),
        F.col("s")[13].cast("double").alias("geoaltitude"),
        F.col("s")[8].cast("boolean").alias("onground"),
        F.lit("live").alias("source"),
        F.lit("live").alias("dataset"),
    )
    # the same state is repeated in consecutive polls if the aircraft did not
    # send a new position: keep one copy per (aircraft, position timestamp)
    return norm.dropDuplicates(["icao24", "time"]).select(*COMMON_COLS)


def build_spark() -> SparkSession:
    return (SparkSession.builder.appName("phase3-kafka-to-minio")
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate())


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", KAFKA_TOPIC)
           .option("startingOffsets", "earliest")
           .option("endingOffsets", "latest")
           .load()
           .select(F.col("value").cast("string").alias("json")))

    live = normalize_live(raw).cache()
    n = live.count()
    print(f"[phase3a] {n} distinct live state vectors read from Kafka topic '{KAFKA_TOPIC}'")
    if n == 0:
        print("[phase3a] Nothing to write (is the producer running?).")
        return

    live.write.mode("overwrite").option("compression", "snappy").parquet(OUT_PATH)
    print(f"[phase3a] Written to {OUT_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
