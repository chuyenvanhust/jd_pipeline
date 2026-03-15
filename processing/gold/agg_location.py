from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, count, round, avg, current_date,
    when, lit, length, concat_ws, collect_list
)
from pyspark.sql.window import Window
from pyspark.sql.functions import rank as spark_rank

# ═══════════════════════════════════════════════════════════
# agg_location.py
# avg_salary tính theo max_salary (không bị kéo về min)
# Output: /datalake/gold/reports/dim_location_trend/
# ═══════════════════════════════════════════════════════════

spark = SparkSession.builder \
    .appName("gold-dim-location") \
    .master("spark://spark-master:7077") \
    .config("spark.network.timeout",            "800s") \
    .config("spark.executor.heartbeatInterval", "60s")  \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

SILVER_PATH = "hdfs://namenode:9000/datalake/silver/topcv/final/"
GOLD_PATH   = "hdfs://namenode:9000/datalake/gold/reports/dim_location_trend/"

df = spark.read.parquet(SILVER_PATH)
total = df.count()
print(f"📥 Silver IT jobs: {total}")

# ── Explode locations ────────────────────────────────────────
df_loc = df \
    .filter(col("locations").isNotNull()) \
    .select(
        col("job_id"),
        col("max_salary"),
        col("technical_skills"),
        explode(col("locations")).alias("city_raw")
    ) \
    .filter(col("city_raw") != "")

# ── Normalize city → nhóm lớn ────────────────────────────────
df_loc = df_loc.withColumn(
    "city",
    when(col("city_raw").contains("Hà Nội"),       lit("Hà Nội"))
    .when(col("city_raw").contains("Hồ Chí Minh"), lit("Hồ Chí Minh"))
    .when(col("city_raw").contains("Đà Nẵng"),     lit("Đà Nẵng"))
    .when(col("city_raw").contains("Toàn quốc"),   lit("Toàn quốc"))
    .otherwise(lit("Khác"))
)

# ── Dùng max_salary ──────────────────────────────────────────
df_with_max = df_loc \
    .withColumn("max_sal", col("max_salary").cast("double")) \
    .filter(col("max_sal") > 0)

# ── Aggregate ────────────────────────────────────────────────
df_city_count = df_loc \
    .groupBy("city") \
    .agg(count("job_id").alias("job_count")) \
    .withColumn("percentage",
        round(col("job_count") / lit(total) * 100, 2))

df_city_salary = df_with_max \
    .groupBy("city") \
    .agg(round(avg("max_sal"), 1).alias("avg_salary"))

# ── Top 5 skills per city — lọc skill hợp lệ (<=40 ký tự) ───
df_skill_city = df_loc \
    .filter(col("technical_skills").isNotNull()) \
    .select("city", explode(col("technical_skills")).alias("skill")) \
    .filter(length(col("skill")) <= 40) \
    .groupBy("city", "skill") \
    .agg(count("*").alias("cnt"))

window = Window.partitionBy("city").orderBy(col("cnt").desc())
df_top_skills = df_skill_city \
    .withColumn("rnk", spark_rank().over(window)) \
    .filter(col("rnk") <= 5) \
    .groupBy("city") \
    .agg(concat_ws(", ", collect_list("skill")).alias("top_skills"))

# ── Join tất cả ──────────────────────────────────────────────
df_result = df_city_count \
    .join(df_city_salary, on="city", how="left") \
    .join(df_top_skills,  on="city", how="left") \
    .withColumn("updated_date", current_date()) \
    .orderBy(col("job_count").desc())

print("\n📍 Location trend:")
df_result.show(truncate=False)

df_result.write.mode("overwrite").parquet(GOLD_PATH)

print(f"\n✅ dim_location_trend ghi tại: {GOLD_PATH}")