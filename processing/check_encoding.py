from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("check-encoding") \
    .master("spark://spark-master:7077") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("\n=== stage_unique ===")
try:
    df1 = spark.read.parquet("hdfs://namenode:9000/datalake/silver/topcv/stage_unique/")
    df1.select("job_id", "title", "location").show(5, truncate=False)
except Exception as e:
    print(f"stage_unique error: {e}")

print("\n=== stage_clean ===")
try:
    df2 = spark.read.parquet("hdfs://namenode:9000/datalake/silver/topcv/stage_clean/")
    df2.select("job_id", "title", "location_raw").show(5, truncate=False)
except Exception as e:
    print(f"stage_clean error: {e}")

print("\n=== silver/final ===")
try:
    df3 = spark.read.parquet("hdfs://namenode:9000/datalake/silver/topcv/final/")
    df3.select("job_id", "title", "locations", "location_raw").show(5, truncate=False)
except Exception as e:
    print(f"silver/final error: {e}")

spark.stop()