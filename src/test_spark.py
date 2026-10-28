from pyspark.sql import SparkSession

def test_spark_connection():
    # Connect to the Spark Master running in Docker
    spark = SparkSession.builder \
        .appName("DockerizedSparkTest") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    
    print("\n--- SPARK CONNECTION SUCCESSFUL ---")
    print(f"Spark Version: {spark.version}")
    
    # Create a simple DataFrame to test processing
    data = [("Flight1", 123.4), ("Flight2", 567.8)]
    columns = ["TransponderID", "Velocity"]
    
    df = spark.createDataFrame(data, columns)
    print("\n--- SAMPLE DATA ---")
    df.show()
    
    spark.stop()

if __name__ == "__main__":
    test_spark_connection()