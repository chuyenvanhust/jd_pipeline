from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("check-ckpt") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("hdfs://namenode:9000/tmp/enrich_ckpt/")

print(f"\n📦 Tổng rows trong checkpoint: {df.count()}")
print(f"Các batch đã xong: {sorted([r._batch_id for r in df.select('_batch_id').distinct().collect()])}")

print("\n--- 5 mẫu đầu ---")
df.select("title", "job_level", "is_it_job", "technical_skills", "locations", "_batch_id") \
  .show(5, truncate=50)