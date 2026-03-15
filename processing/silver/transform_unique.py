from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StringType, ArrayType
)

# ─────────────────────────────────────────
# 1. SPARK SESSION
# ─────────────────────────────────────────
spark = SparkSession.builder \
    .appName("topcv-silver-dedup") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────
# 2. SCHEMA — 14 fields từ Bronze JSON
# ─────────────────────────────────────────
schema = StructType() \
    .add("job_id",                  StringType()) \
    .add("title",                   StringType()) \
    .add("company",                 StringType()) \
    .add("url",                     StringType()) \
    .add("salary",                  StringType()) \
    .add("location",                StringType()) \
    .add("experience",              StringType()) \
    .add("tags",                    ArrayType(StringType())) \
    .add("tags_requirement",        ArrayType(StringType())) \
    .add("tags_specialization",     ArrayType(StringType())) \
    .add("mo_ta_cong_viec_html",    StringType()) \
    .add("yeu_cau_ung_vien_html",   StringType()) \
    .add("quyen_loi_html",          StringType()) \
    .add("deadline",                StringType())

# ─────────────────────────────────────────
# 3. ĐỌC STREAM TỪ KAFKA
# ─────────────────────────────────────────
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "job_raw") \
    .option("startingOffsets", "earliest") \
    .load()

# ─────────────────────────────────────────
# 4. PARSE JSON — bytes → các cột
# ─────────────────────────────────────────
df_parsed = df_raw \
    .select(
        from_json(col("value").cast("string"), schema).alias("data")
    ) \
    .select("data.*")

# ─────────────────────────────────────────
# 5. DEDUPLICATION — giữ job_id đầu tiên
# ─────────────────────────────────────────
# Lưu ý: dropDuplicates trong streaming cần watermark
# hoặc dùng withWatermark + dropDuplicatesWithinWatermark
# Ở đây dùng dropDuplicates đơn giản vì data crawl 1 lần/ngày
df_unique = df_parsed.dropDuplicates(["job_id"])

# ─────────────────────────────────────────
# 6. GHI RA HDFS SILVER (Parquet)
# ─────────────────────────────────────────
SILVER_PATH     = "hdfs://namenode:9000/datalake/silver/topcv/stage_unique/"
CHECKPOINT_PATH = "hdfs://namenode:9000/datalake/checkpoints/silver_unique/"

query = df_unique.writeStream \
    .format("parquet") \
    .option("path", SILVER_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(availableNow=True) \
    .start()

print("⏳ Spark đang xử lý...")
query.awaitTermination()
print(f"✅ Xong! Data ghi tại: {SILVER_PATH}")