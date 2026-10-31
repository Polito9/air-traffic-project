"""Phase 3b: Clean noisy pings, group them into flight sessions and store
continuous GeoJSON LineString trajectories in MongoDB.

Inputs (MinIO, bucket airtraffic-raw):
  - state_vectors/states_<year>.parquet  historical files from Phase 2
    (NOTE: these are SIMULATED data; they are labelled dataset=synthetic_<year>)
  - state_vectors/live/                  output of phase3_kafka_to_minio.py
                                         (real data, dataset=live)

Steps (as in the project plan):
  1. Noise filtering: drop rows with missing/invalid coordinates and
     physically impossible speeds (> Mach 1, 343 m/s).
  2. Sessionization: per aircraft (icao24) a new flight session starts when
     the gap between consecutive pings is > 30 minutes.
  3. Store: cleaned points -> s3a://airtraffic-processed/points_clean/ (Parquet)
            trajectories  -> MongoDB airtraffic_db.flight_trajectories
     Each trajectory document has a GeoJSON `geometry` (LineString,
     [lon, lat] order) plus per-point arrays (times, headings, ...) that
     Phase 4 needs for holding-pattern detection.

Run INSIDE the Spark master container, e.g.:
    spark-submit --conf spark.jars.ivy=/tmp/.ivy \
      --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
      /opt/bitnami/spark/src/phase3_clean_sessionize.py
"""
import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
RAW_BUCKET = os.getenv("RAW_BUCKET", "airtraffic-raw")
PROCESSED_BUCKET = os.getenv("PROCESSED_BUCKET", "airtraffic-processed")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB = os.getenv("MONGO_DB", "airtraffic_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "flight_trajectories")
WRITE_MONGO = os.getenv("WRITE_MONGO", "1") == "1"

HIST_PATH = f"s3a://{RAW_BUCKET}/state_vectors/states_*.parquet"
LIVE_PATH = f"s3a://{RAW_BUCKET}/state_vectors/live/"
POINTS_OUT = f"s3a://{PROCESSED_BUCKET}/points_clean/"

MAX_SPEED_MS = 343.0          # Mach 1 at sea level
SESSION_GAP_S = 30 * 60       # new flight session after a 30 min gap
MIN_POINTS = 2

COMMON_COLS = ["icao24", "callsign", "time", "lat", "lon", "velocity", "heading",
               "vertrate", "baroaltitude", "geoaltitude", "onground",
               "source", "dataset"]


def to_common(df: DataFrame) -> DataFrame:
    """Cast to the common schema."""
    return df.select(
        F.col("icao24").cast("string").alias("icao24"),
        F.trim(F.col("callsign").cast("string")).alias("callsign"),
        F.col("time").cast("long").alias("time"),
        F.col("lat").cast("double").alias("lat"),
        F.col("lon").cast("double").alias("lon"),
        F.col("velocity").cast("double").alias("velocity"),
        F.col("heading").cast("double").alias("heading"),
        F.col("vertrate").cast("double").alias("vertrate"),
        F.col("baroaltitude").cast("double").alias("baroaltitude"),
        F.col("geoaltitude").cast("double").alias("geoaltitude"),
        F.col("onground").cast("boolean").alias("onground"),
        F.col("source").cast("string").alias("source"),
        F.col("dataset").cast("string").alias("dataset"),
    )


def load_historical(spark: SparkSession, path: str = HIST_PATH) -> DataFrame:
    """Phase 2 historical files. They are simulated: label them as such."""
    df = (spark.read.parquet(path)
          .withColumn("_year", F.regexp_extract(F.input_file_name(), r"states_(\d{4})", 1))
          .withColumn("source", F.lit("synthetic"))
          .withColumn("dataset", F.concat(F.lit("synthetic_"), F.col("_year")))
          .drop("_year"))
    return to_common(df)


def load_live(spark: SparkSession, path: str = LIVE_PATH) -> DataFrame:
    return to_common(spark.read.parquet(path))


def clean(df: DataFrame):
    """Return (clean_df, stats_dict). Removes noise and duplicates."""
    total = df.count()
    has_coords = (F.col("lat").isNotNull() & F.col("lon").isNotNull()
                  & F.col("lat").between(-90, 90) & F.col("lon").between(-180, 180)
                  & F.col("time").isNotNull() & F.col("icao24").isNotNull())
    step1 = df.filter(has_coords)
    n1 = step1.count()

    # physically impossible speed (> Mach 1); null speed is kept (unknown)
    step2 = step1.filter(F.col("velocity").isNull() | (F.col("velocity") <= MAX_SPEED_MS))
    n2 = step2.count()

    step3 = step2.dropDuplicates(["dataset", "icao24", "time"])
    n3 = step3.count()

    stats = {
        "input_rows": total,
        "dropped_missing_or_invalid_coords": total - n1,
        "dropped_speed_over_mach1": n1 - n2,
        "dropped_duplicates": n2 - n3,
        "clean_rows": n3,
    }
    return step3, stats


def sessionize(df: DataFrame, gap_s: int = SESSION_GAP_S) -> DataFrame:
    """Add `session_idx` (per aircraft and dataset) and `session_id`.
    Also drops consecutive repeated positions inside a session (an aircraft
    parked on the ground would otherwise create zero-length segments, which
    MongoDB's 2dsphere index rejects)."""
    w = Window.partitionBy("dataset", "icao24").orderBy("time")
    w_cum = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    d = df.withColumn("_prev_time", F.lag("time").over(w))
    d = d.withColumn(
        "_new", F.when(F.col("_prev_time").isNull()
                       | ((F.col("time") - F.col("_prev_time")) > gap_s), 1).otherwise(0))
    d = d.withColumn("session_idx", F.sum("_new").over(w_cum)).drop("_prev_time", "_new")

    ws = Window.partitionBy("dataset", "icao24", "session_idx").orderBy("time")
    d = (d.withColumn("_plat", F.lag("lat").over(ws))
          .withColumn("_plon", F.lag("lon").over(ws))
          .filter(F.col("_plat").isNull()
                  | (F.col("lat") != F.col("_plat"))
                  | (F.col("lon") != F.col("_plon")))
          .drop("_plat", "_plon"))

    return d.withColumn(
        "session_id",
        F.concat_ws(":", F.col("dataset"), F.col("icao24"), F.col("session_idx").cast("string")))


def build_trajectories(sessions: DataFrame, min_points: int = MIN_POINTS) -> DataFrame:
    """One document per flight session with a GeoJSON LineString."""
    pts = F.sort_array(F.collect_list(F.struct(
        "time", "lon", "lat", "heading", "velocity", "baroaltitude", "vertrate")))
    g = (sessions.groupBy("dataset", "source", "icao24", "session_idx", "session_id")
         .agg(pts.alias("pts"), F.max("callsign").alias("callsign")))

    g = g.withColumn("coordinates", F.transform("pts", lambda p: F.array(p["lon"], p["lat"])))
    g = g.filter(F.size("coordinates") >= min_points)

    return g.select(
        F.concat_ws(":", F.col("session_id"),
                    F.element_at("pts", 1)["time"].cast("string")).alias("_id"),
        "icao24", "callsign", "dataset", "source", "session_id",
        F.element_at("pts", 1)["time"].alias("start_time"),
        F.element_at("pts", -1)["time"].alias("end_time"),
        F.size("pts").alias("n_points"),
        F.struct(F.lit("LineString").alias("type"),
                 F.col("coordinates").alias("coordinates")).alias("geometry"),
        F.transform("pts", lambda p: p["time"]).alias("times"),
        F.transform("pts", lambda p: p["heading"]).alias("headings"),
        F.transform("pts", lambda p: p["velocity"]).alias("velocities"),
        F.transform("pts", lambda p: p["baroaltitude"]).alias("altitudes"),
    )


def build_spark() -> SparkSession:
    return (SparkSession.builder.appName("phase3-clean-sessionize")
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

    parts = [load_historical(spark)]
    try:
        parts.append(load_live(spark))
    except Exception as exc:  # live folder may not exist yet
        print(f"[phase3b] No live data found ({type(exc).__name__}); using historical only.")
    raw = parts[0]
    for p in parts[1:]:
        raw = raw.unionByName(p)

    cleaned, stats = clean(raw)
    print("[phase3b] Cleaning stats:", stats)

    sessions = sessionize(cleaned).cache()
    sessions.write.mode("overwrite").partitionBy("dataset").parquet(POINTS_OUT)
    print(f"[phase3b] Cleaned points written to {POINTS_OUT}")

    traj = build_trajectories(sessions).cache()
    print(f"[phase3b] {traj.count()} trajectories (flight sessions) built")
    traj.groupBy("dataset").count().orderBy("dataset").show(truncate=False)

    if WRITE_MONGO:
        (traj.write.format("mongodb")
         .mode("append")                       # _id is deterministic -> re-runs replace
         .option("connection.uri", MONGO_URI)
         .option("database", MONGO_DB)
         .option("collection", MONGO_COLLECTION)
         .option("operationType", "replace")
         .save())
        print(f"[phase3b] Trajectories saved to {MONGO_DB}.{MONGO_COLLECTION}")
    spark.stop()


if __name__ == "__main__":
    main()
