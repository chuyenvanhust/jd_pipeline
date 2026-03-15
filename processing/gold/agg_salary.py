from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, round, avg, min as spark_min,
    max as spark_max, current_date
)

# ═══════════════════════════════════════════════════════════
# agg_salary.py
# avg_salary tính theo max_salary (không bị kéo về min)
# Output: /datalake/gold/reports/fact_salary_benchmark/
# ═══════════════════════════════════════════════════════════

spark = SparkSession.builder \
    .appName("gold-fact-salary") \
    .master("spark://spark-master:7077") \
    .config("spark.network.timeout",            "800s") \
    .config("spark.executor.heartbeatInterval", "60s")  \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

SILVER_PATH = "hdfs://namenode:9000/datalake/silver/topcv/final/"
GOLD_PATH   = "hdfs://namenode:9000/datalake/gold/reports/fact_salary_benchmark/"

df = spark.read.parquet(SILVER_PATH)
print(f"📥 Silver IT jobs: {df.count()}")

# ── Chỉ lấy job có salary xác định và job_level rõ ràng ─────
df_clean = df.filter(
    (col("min_salary") > 0) &
    (col("max_salary") > 0) &
    (col("job_level") != "Unknown") &
    col("job_level").isNotNull()
).withColumn(
    "max_sal",
    col("max_salary").cast("double")
).withColumn(
    "years_of_experience",
    col("exp_min").cast("int")
)

print(f"   Jobs có salary + level rõ: {df_clean.count()}")

# ── Aggregate theo job_level ─────────────────────────────────
df_by_level = df_clean \
    .groupBy("job_level") \
    .agg(
        count("job_id").alias("job_count"),
        round(avg("max_sal"),                        1).alias("avg_salary"),
        round(spark_min("min_salary").cast("double"), 1).alias("min_salary"),
        round(spark_max("max_salary").cast("double"), 1).alias("max_salary"),
        round(avg("years_of_experience"),             1).alias("years_of_experience"),
    ) \
    .withColumn("updated_date", current_date()) \
    .orderBy(col("avg_salary").asc())

print("\n💰 Salary benchmark theo job_level:")
df_by_level.show(truncate=False)

df_by_level.write.mode("overwrite").parquet(GOLD_PATH)

print(f"\n✅ fact_salary_benchmark ghi tại: {GOLD_PATH}")