from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, current_date, lit, when, size
from pyspark.sql.types import (
    StructType, StringType, ArrayType,
    IntegerType, FloatType, BooleanType
)
import json, requests, math

# ═══════════════════════════════════════════════════════════
# CẤU HÌNH
# ═══════════════════════════════════════════════════════════
OLLAMA_URL      = "http://ollama:11434/api/generate"
OLLAMA_MODEL    = "llama3.2:3b"
BATCH_SIZE      = 200
CHECKPOINT_PATH = "hdfs://namenode:9000/tmp/enrich_ckpt/"
SILVER_PATH     = "hdfs://namenode:9000/datalake/silver/topcv/final/"
STAGE_PATH      = "hdfs://namenode:9000/datalake/silver/topcv/stage_clean/"
NUM_PARTITIONS  = 8

# ═══════════════════════════════════════════════════════════
# 1. SPARK SESSION
# ═══════════════════════════════════════════════════════════
spark = SparkSession.builder \
    .appName("topcv-silver-enrich") \
    .master("spark://spark-master:7077") \
    .config("spark.network.timeout",            "800s")  \
    .config("spark.executor.heartbeatInterval", "60s")   \
    .config("spark.sql.shuffle.partitions",     str(NUM_PARTITIONS)) \
    .config("spark.executor.memory",            "1800m") \
    .config("spark.driver.memory",              "1g")    \
    .config("spark.speculation",                "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ═══════════════════════════════════════════════════════════
# 2. JOB_LEVEL — xử lý hoàn toàn bằng Python, không dùng LLM
#    Lý do: LLM 3b không đủ khả năng áp rule cứng nhất quán,
#    dễ bị ảnh hưởng bởi nội dung JD → overclassify Senior
# ═══════════════════════════════════════════════════════════
def get_level_from_title(title: str):
    """
    Rule cứng từ title — ưu tiên cao nhất, chính xác 100%.
    Trả về None nếu title không có hint rõ ràng.
    """
    if not title:
        return None
    t = title.lower()
    if any(x in t for x in ["senior", "sr.", " sr "]):  return "Senior"
    if any(x in t for x in ["junior", "jr.", " jr "]):  return "Junior"
    if "middle" in t or " mid " in t:                   return "Middle"
    if any(x in t for x in ["lead", "tech lead"]):      return "Lead"
    if "manager" in t:                                   return "Manager"
    if any(x in t for x in ["intern", "thực tập"]):     return "Intern"
    return None

def get_level_from_exp(exp_min) -> str:
    """
    Fallback khi title không có hint.
    Ngưỡng cao hơn prompt cũ để tránh overclassify Senior:
      >= 7 năm → Senior  (thay vì >= 5)
      >= 3 năm → Middle  (thay vì >= 2)
      <  3 năm → Junior
    """
    if exp_min is None or exp_min < 0:
        return "Unknown"
    if exp_min >= 7:
        return "Senior"
    if exp_min >= 3:
        return "Middle"
    return "Junior"

def resolve_job_level(title, exp_min) -> str:
    """Ưu tiên: title rule > exp_min fallback"""
    return get_level_from_title(title) or get_level_from_exp(exp_min)

# ═══════════════════════════════════════════════════════════
# 3. PROMPT — bỏ JOB_LEVEL, prompt ngắn hơn → LLM tập trung hơn
# ═══════════════════════════════════════════════════════════
PROMPT_TEMPLATE = """Bạn là chuyên gia phân tích JD IT Việt Nam.
Trả về JSON ONLY, không markdown, không giải thích.

=== INPUT ===
Tiêu đề: {title}
salary_type: {salary_type} | min_salary: {min_salary} | max_salary: {max_salary}
exp_min: {exp_min} | exp_type: {exp_type}
is_it_preliminary: {is_it_preliminary}
location_raw: {location_raw}
Mô tả: {mo_ta}
Yêu cầu: {yeu_cau}
Quyền lợi: {quyen_loi}

=== NHIỆM VỤ ===

1. TECHNICAL_SKILLS
   - Trích từ mô tả + yêu cầu, KHÔNG từ tags
   - Normalize: "ReactJS"/"React.js"/"React" → "React"
   - Chỉ kỹ năng kỹ thuật, không soft skills

2. IS_IT_JOB
   - Chỉ quyết định nếu is_it_preliminary = "none"
   - Nếu "true" hoặc "false" → giữ nguyên, không đổi
   - Scope: "trực tiếp tham gia sản xuất sản phẩm CNTT"
     true:  Dev, QA, DevOps, Data, BA phần mềm, BrSE, UX/UI web, Security
     false: Sales, Designer đồ họa, Giáo viên, CNC/PLC thuần cơ khí

3. EXP_MAX
   - Đọc yêu cầu để lấy giới hạn trên, VD "3-5 năm" → 5
   - Nếu không có → -1.0

4. LOCATIONS
   - Đọc location_raw, trích tên tỉnh/thành chuẩn hành chính Việt Nam
   - Bỏ: "nơi khác", "địa điểm khác", số đếm, "remote"
   - Nếu "toàn quốc" hoặc không xác định được → ["Toàn quốc"]
   - VD: "Hà Nội & 3 nơi khác" → ["Hà Nội"]
         "TP.HCM" → ["Hồ Chí Minh"]
         "Remote toàn quốc" → ["Toàn quốc"]

5. MAX_SALARY_VERIFIED
   - Chỉ áp dụng khi salary_type = "negotiable"
   - Đọc quyền lợi xem có mention số lương cụ thể không
   - VD: "upto 35M" → 35 | không có → -1

=== OUTPUT ===
{{"technical_skills":[],"is_it_job":true,"exp_max":-1.0,"locations":[],"max_salary_verified":-1}}"""

# ═══════════════════════════════════════════════════════════
# 4. OLLAMA CALLER
# ═══════════════════════════════════════════════════════════
def call_ollama(title, mo_ta, yeu_cau, quyen_loi,
                salary_type, min_salary, max_salary,
                exp_min, exp_type,
                is_it_preliminary, location_raw) -> dict:

    DEFAULT = {
        "technical_skills":    [],
        "is_it_job":           False,
        "exp_max":             -1.0,
        "locations":           [],
        "max_salary_verified": -1,
    }

    prompt = PROMPT_TEMPLATE.format(
        title              = title             or "",
        salary_type        = salary_type       or "unknown",
        min_salary         = min_salary        if min_salary != -1   else "không rõ",
        max_salary         = max_salary        if max_salary != -1   else "không rõ",
        exp_min            = exp_min           if exp_min   != -1.0  else "không rõ",
        exp_type           = exp_type          or "unknown",
        is_it_preliminary  = is_it_preliminary or "none",
        location_raw       = location_raw      or "",
        mo_ta              = (mo_ta     or "")[:600],
        yeu_cau            = (yeu_cau   or "")[:600],
        quyen_loi          = (quyen_loi or "")[:300],
    )

    for attempt in range(2):
        try:
            resp = requests.post(
                OLLAMA_URL,
                json={
                    "model":  OLLAMA_MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.1, "num_predict": 256},
                },
                timeout=20
            )
            raw = resp.json().get("response", "{}").strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            return json.loads(raw.strip())

        except requests.exceptions.Timeout:
            if attempt == 1:
                return DEFAULT
            continue
        except Exception:
            return DEFAULT

    return DEFAULT

# ═══════════════════════════════════════════════════════════
# 5. SCHEMA — bỏ job_level khỏi LLM output
# ═══════════════════════════════════════════════════════════
enrich_schema = StructType() \
    .add("technical_skills",    ArrayType(StringType())) \
    .add("is_it_job",           BooleanType()) \
    .add("exp_max",             FloatType()) \
    .add("locations",           ArrayType(StringType())) \
    .add("max_salary_verified", IntegerType())

# ═══════════════════════════════════════════════════════════
# 6. UDF ENRICH
# ═══════════════════════════════════════════════════════════
def enrich_job(title, mo_ta, yeu_cau, quyen_loi,
               salary_type, min_salary, max_salary,
               exp_min, exp_type,
               is_it_preliminary, location_raw):

    r = call_ollama(
        title, mo_ta, yeu_cau, quyen_loi,
        salary_type, min_salary, max_salary,
        exp_min, exp_type,
        is_it_preliminary, location_raw
    )

    if is_it_preliminary == "true":    is_it = True
    elif is_it_preliminary == "false": is_it = False
    else:                              is_it = bool(r.get("is_it_job", False))

    return (
        r.get("technical_skills",    []),
        is_it,
        float(r.get("exp_max",       -1.0)),
        r.get("locations",           []),
        int(r.get("max_salary_verified", -1) or -1),  # ← thêm "or -1"
    )
enrich_udf = udf(enrich_job, enrich_schema)

# job_level UDF riêng — nhẹ, không gọi Ollama
job_level_udf = udf(
    lambda title, exp_min: resolve_job_level(title, exp_min),
    StringType()
)

# ═══════════════════════════════════════════════════════════
# 7. ĐỌC DATA
# ═══════════════════════════════════════════════════════════
df = spark.read.parquet(STAGE_PATH)

df_no_content  = df.filter(col("has_content") == False).cache()
df_has_content = df.filter(col("has_content") == True).cache()

skip_no_content = df_no_content.count()
print(f"📥 Tổng jobs đọc vào       : {df.count()}")
print(f"⏭ Skip (no content)       : {skip_no_content}")

df_non_it    = df_has_content.filter(col("is_it_preliminary") == "false").cache()
df_to_enrich = df_has_content.filter(col("is_it_preliminary") != "false").cache()

skip_non_it  = df_non_it.count()
total_enrich = df_to_enrich.count()
print(f"⏭ Skip (non-IT rule)      : {skip_non_it}")
print(f"🤖 Gọi Ollama cho         : {total_enrich} jobs")

# ═══════════════════════════════════════════════════════════
# 8. RESUME
# ═══════════════════════════════════════════════════════════
def get_done_batches() -> set:
    try:
        done_df = spark.read.parquet(CHECKPOINT_PATH)
        done = {int(r["_batch_id"])
                for r in done_df.select("_batch_id").distinct().collect()}
        print(f"♻️  Resume: tìm thấy {len(done)} batch đã xong → {sorted(done)}")
        return done
    except Exception:
        print("🆕 Chưa có checkpoint — bắt đầu từ batch 0")
        return set()

done_batches = get_done_batches()

# ═══════════════════════════════════════════════════════════
# 9. CHẠY TỪNG BATCH
# ═══════════════════════════════════════════════════════════
rows        = df_to_enrich.collect()
num_batches = math.ceil(total_enrich / BATCH_SIZE)

print(f"\n🔢abcbdsijfjhdsfjhsefj Batch size: {BATCH_SIZE} | Số batch: {num_batches} | Partitions/batch: {NUM_PARTITIONS}")
print("─" * 60)

for batch_idx in range(num_batches):

    if batch_idx in done_batches:
        start = batch_idx * BATCH_SIZE
        end   = min(start + BATCH_SIZE, total_enrich)
        print(f"⏭ Batch {batch_idx+1:>2}/{num_batches} ({end-start} jobs) — đã có, bỏ qua")
        continue

    start      = batch_idx * BATCH_SIZE
    end        = min(start + BATCH_SIZE, total_enrich)
    batch_rows = rows[start:end]

    print(f"🔄 Batch {batch_idx+1:>2}/{num_batches} ({len(batch_rows)} jobs) ...")

    df_batch = spark.createDataFrame(batch_rows, schema=df_to_enrich.schema) \
                    .repartition(NUM_PARTITIONS)

    df_batch_enriched = df_batch.withColumn(
        "_enriched",
        enrich_udf(
            col("title"), col("mo_ta_text"), col("yeu_cau_text"),
            col("quyen_loi_text"), col("salary_type"),
            col("min_salary"), col("max_salary"),
            col("exp_min"), col("exp_type"),
            col("is_it_preliminary"), col("location_raw"),
        )
    ) \
    .withColumn("technical_skills",    col("_enriched.technical_skills")) \
    .withColumn("is_it_job",           col("_enriched.is_it_job")) \
    .withColumn("exp_max",             col("_enriched.exp_max")) \
    .withColumn("locations",           col("_enriched.locations")) \
    .withColumn("max_salary_verified", col("_enriched.max_salary_verified")) \
    .withColumn("job_level",           job_level_udf(col("title"), col("exp_min"))) \
    .withColumn("classification_source",
        when(col("is_it_preliminary") == "none", lit("llm"))
        .otherwise(lit("rule"))
    ) \
    .withColumn("_batch_id", lit(batch_idx)) \
    .drop("_enriched")

    df_batch_enriched.write \
        .mode("append") \
        .parquet(CHECKPOINT_PATH)

    pct = (batch_idx + 1) / num_batches * 100
    print(f"   ✅ Xong | Tiến độ: {batch_idx+1}/{num_batches} ({pct:.0f}%)")

print("\n" + "─" * 60)
print("📦 Tất cả batch xong — gộp lại và ghi silver/final ...")

# ═══════════════════════════════════════════════════════════
# 10. GỘP CHECKPOINT → FILTER IT → GHI FINAL
# ═══════════════════════════════════════════════════════════
df_all = spark.read.parquet(CHECKPOINT_PATH).drop("_batch_id")

df_it = df_all.filter(col("is_it_job") == True) \
    .withColumn("ingestion_date", current_date()) \
    .withColumn("source",         lit("topcv"))

df_it.write \
    .mode("overwrite") \
    .partitionBy("ingestion_date") \
    .parquet(SILVER_PATH)

# ═══════════════════════════════════════════════════════════
# 11. DATA QUALITY REPORT
# ═══════════════════════════════════════════════════════════
total_it  = df_it.count()
unk_level = df_it.filter(col("job_level") == "Unknown").count()
empty_sk  = df_it.filter(
    col("technical_skills").isNull() | (size(col("technical_skills")) == 0)
).count()
neg_sal   = df_it.filter(col("min_salary") == -1).count()
src_rule  = df_it.filter(col("classification_source") == "rule").count()
src_llm   = df_it.filter(col("classification_source") == "llm").count()

print(f"\n📊 DATA QUALITY REPORT — silver/final")
print(f"   IT jobs giữ lại        : {total_it}")
print(f"   job_level Unknown      : {unk_level}  ({unk_level / max(total_it,1):.1%})")
print(f"   skills rỗng            : {empty_sk}  ({empty_sk  / max(total_it,1):.1%})")
print(f"   salary không xác định  : {neg_sal}  ({neg_sal   / max(total_it,1):.1%})")
print(f"   phân loại bởi rule     : {src_rule}")
print(f"   phân loại bởi LLM      : {src_llm}")

print(f"\n   Phân bố job_level:")
df_it.groupBy("job_level").count().orderBy(col("count").desc()).show(10, truncate=False)

if unk_level / max(total_it, 1) > 0.3:
    print("⚠ CẢNH BÁO: >30% job_level Unknown — kiểm tra lại rule")
if empty_sk / max(total_it, 1) > 0.2:
    print("⚠ CẢNH BÁO: >20% skills rỗng — kiểm tra lại nội dung JD")

print(f"\n✅ Xong! Silver final ghi tại: {SILVER_PATH}")
df_it.printSchema()