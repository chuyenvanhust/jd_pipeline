from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, count, round, avg, current_date, lit, length
)

# ═══════════════════════════════════════════════════════════
# agg_skills.py
# avg_salary tính theo max_salary (không bị kéo về min)
# Lọc skill name quá dài (>40 ký tự) → không phải tên skill
# Output: /datalake/gold/reports/dim_skills_demand/
# ═══════════════════════════════════════════════════════════

spark = SparkSession.builder \
    .appName("gold-dim-skills") \
    .master("spark://spark-master:7077") \
    .config("spark.network.timeout",            "800s") \
    .config("spark.executor.heartbeatInterval", "60s")  \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

SILVER_PATH = "hdfs://namenode:9000/datalake/silver/topcv/final/"
GOLD_PATH   = "hdfs://namenode:9000/datalake/gold/reports/dim_skills_demand/"

df = spark.read.parquet(SILVER_PATH)
total_it = df.count()
print(f"📥 Silver IT jobs: {total_it}")

# ── Explode + lọc skill hợp lệ ──────────────────────────────
# Bỏ: chuỗi dài > 40 ký tự (thường là câu mô tả, không phải skill)
df_skills = df \
    .filter(col("technical_skills").isNotNull()) \
    .select(
        col("job_id"),
        col("max_salary"),
        explode(col("technical_skills")).alias("skill_name")
    ) \
    .filter(col("skill_name") != "") \
    .filter(length(col("skill_name")) <= 40)

print(f"   Rows sau lọc skill hợp lệ: {df_skills.count()}")

# ── Dùng max_salary thay midpoint ───────────────────────────
df_with_max = df_skills \
    .withColumn("max_sal", col("max_salary").cast("double")) \
    .filter(col("max_sal") > 0)

# ── Aggregate ───────────────────────────────────────────────
df_skill_count = df_skills \
    .groupBy("skill_name") \
    .agg(count("job_id").alias("job_count")) \
    .withColumn("percentage",
        round(col("job_count") / lit(total_it) * 100, 2))

df_skill_salary = df_with_max \
    .groupBy("skill_name") \
    .agg(round(avg("max_sal"), 1).alias("avg_salary"))

df_result = df_skill_count \
    .join(df_skill_salary, on="skill_name", how="left") \
    .withColumn("updated_date", current_date()) \
    .orderBy(col("job_count").desc())

print("\n🏆 Top 20 skills:")
df_result.show(20, truncate=False)

df_result.write.mode("overwrite").parquet(GOLD_PATH)

total_skills = df_result.count()
print(f"\n✅ dim_skills_demand ghi tại: {GOLD_PATH}")
print(f"   Tổng skills sau lọc: {total_skills}")