"""Phase 4a - Holding pattern detection (PySpark).

Reads s3a://airtraffic-processed/points_clean/, and for every flight session
tracks the signed heading change between consecutive pings. A holding is flagged
when, inside a sliding 20-minute window, the aircraft accumulates >= 360 degrees
of turn (same direction) while staying inside a small area (<= 0.5 deg box,
~50 km). Consecutive flagged pings of a session are merged into ONE event, saved
in MongoDB airtraffic_db.holding_events (GeoJSON Point = centre of the loop).

Env: MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, PROCESSED_BUCKET,
     MONGO_URI, WRITE_MONGO=0 to skip Mongo, POINTS_PATH to override input.
"""
import os
from pyspark.sql import SparkSession, DataFrame, Window, functions as F

TURN_DEG = 360.0
WINDOW_S = 1200          # look-back window (20 min)
MAX_BOX_DEG = 0.5        # max lat/lon extent of the loop
MIN_SPEED_MS = 75.0      # ~145 kt: drops helicopters, gliders, ground jitter
MIN_EVENT_POINTS = 6     # ignore blips of 1-5 pings
MIN_EVENT_S = 120        # event must last at least 2 minutes

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
ACCESS = os.getenv("MINIO_ACCESS_KEY", "admin")
SECRET = os.getenv("MINIO_SECRET_KEY", "password123")
BUCKET = os.getenv("PROCESSED_BUCKET", "airtraffic-processed")
POINTS = os.getenv("POINTS_PATH", f"s3a://{BUCKET}/points_clean/")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")


def detect_holdings(points: DataFrame) -> DataFrame:
    p = points.where(F.col("heading").isNotNull() & ~F.coalesce(F.col("onground"), F.lit(False))
                     & (F.coalesce(F.col("velocity"), F.lit(0.0)) >= MIN_SPEED_MS))
    w = Window.partitionBy("session_id").orderBy("time")
    p = p.withColumn("_prev_h", F.lag("heading").over(w))
    # signed shortest turn in (-180, 180]
    d = F.col("heading") - F.col("_prev_h")
    d = F.when(d > 180, d - 360).when(d <= -180, d + 360).otherwise(d)
    p = p.withColumn("dturn", F.coalesce(d, F.lit(0.0)))
    wr = Window.partitionBy("session_id").orderBy("time").rangeBetween(-WINDOW_S, 0)
    p = (p.withColumn("turn_sum", F.sum("dturn").over(wr))
          .withColumn("box_lat", F.max("lat").over(wr) - F.min("lat").over(wr))
          .withColumn("box_lon", F.max("lon").over(wr) - F.min("lon").over(wr)))
    flag = (F.abs("turn_sum") >= TURN_DEG) & (F.col("box_lat") <= MAX_BOX_DEG) & (F.col("box_lon") <= MAX_BOX_DEG)
    p = p.withColumn("is_hold", flag)
    # merge consecutive flagged pings: a new event starts when is_hold goes False->True
    p = p.withColumn("_pf", F.lag("is_hold").over(w))
    p = p.withColumn("_start", (F.col("is_hold") & (~F.coalesce("_pf", F.lit(False)))).cast("int"))
    p = p.withColumn("event_idx", F.sum("_start").over(w.rowsBetween(Window.unboundedPreceding, 0)))
    h = p.where("is_hold")
    ev = (h.groupBy("dataset", "source", "icao24", "session_id", "event_idx")
            .agg(F.first("callsign", ignorenulls=True).alias("callsign"),
                 F.min("time").alias("start_time"), F.max("time").alias("end_time"),
                 F.count("*").alias("n_points"),
                 F.avg("lat").alias("lat"), F.avg("lon").alias("lon"),
                 F.max(F.abs("turn_sum")).alias("max_turn_deg"),
                 F.avg(F.coalesce("baroaltitude", "geoaltitude")).alias("avg_altitude")))
    ev = ev.where((F.col("n_points") >= MIN_EVENT_POINTS) & ((F.col("end_time") - F.col("start_time")) >= MIN_EVENT_S))
    return (ev.withColumn("_id", F.concat_ws(":", "session_id", F.col("event_idx").cast("string")))
              .withColumn("duration_s", F.col("end_time") - F.col("start_time"))
              .withColumn("geometry", F.struct(F.lit("Point").alias("type"),
                                               F.array("lon", "lat").alias("coordinates"))))


def build_spark():
    return (SparkSession.builder.appName("phase4-holdings")
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", ACCESS)
            .config("spark.hadoop.fs.s3a.secret.key", SECRET)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.mongodb.write.connection.uri", MONGO_URI)
            .config("spark.mongodb.write.database", "airtraffic_db")
            .config("spark.mongodb.write.collection", "holding_events")
            .getOrCreate())


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    pts = spark.read.parquet(POINTS)
    ev = detect_holdings(pts).cache()
    n = ev.count()
    print(f"Holding events detected: {n}")
    ev.groupBy("dataset").count().orderBy("dataset").show(truncate=False)
    ev.select("dataset", "callsign", "icao24", "n_points", "duration_s", "max_turn_deg", "lat", "lon").show(10, truncate=False)
    if os.getenv("WRITE_MONGO", "1") != "0" and n > 0:
        (ev.drop("event_idx", "lat", "lon").write.format("mongodb").mode("append")
           .option("operationType", "replace").save())
        print("Saved to airtraffic_db.holding_events")
    spark.stop()


if __name__ == "__main__":
    main()
