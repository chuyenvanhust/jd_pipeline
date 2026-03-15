from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, length, when
from pyspark.sql.types import (
    StructType, StringType, ArrayType,
    IntegerType, FloatType, DateType
)
import re
import html
import math
from datetime import datetime

# ─────────────────────────────────────────
# 1. SPARK SESSION
# ─────────────────────────────────────────
spark = SparkSession.builder \
    .appName("topcv-silver-clean") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────
# 2. SCHEMA — FIX: yeu_cau_ung_vien_html (vien, không phải viec)
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
# 3. UDF: CLEAN HTML → PLAIN TEXT
# ─────────────────────────────────────────
def clean_html_fn(raw_html: str) -> str:
    if not raw_html:
        return ""
    text = raw_html
    text = re.sub(r'<li[^>]*>',    '\n• ', text)
    text = re.sub(r'<p[^>]*>',     '\n',   text)
    text = re.sub(r'<br\s*/?>',    '\n',   text)
    text = re.sub(r'</ul>|</ol>',  '\n',   text)
    text = re.sub(r'<[^>]+>',      '',     text)
    text = html.unescape(text)
    lines = [line.strip() for line in text.splitlines()]
    cleaned = []
    prev_empty = False
    for line in lines:
        if line == "":
            if not prev_empty:
                cleaned.append(line)
            prev_empty = True
        else:
            cleaned.append(line)
            prev_empty = False
    return '\n'.join(cleaned).strip()

clean_html_udf = udf(clean_html_fn, StringType())

# ─────────────────────────────────────────
# 4. UDF: PARSE SALARY
# Tỷ giá cứng — lưu salary_raw để recalculate nếu cần
# ─────────────────────────────────────────
USD_RATE = 0.026    # 1 USD = 26,000 VND = 0.026 triệu VND
JPY_RATE = 0.000165 # 1 JPY = 165 VND   = 0.000165 triệu VND

def parse_salary(s: str):
    """
    Trả về (min_salary, max_salary, salary_currency, salary_type)
    Đơn vị output: triệu VND, làm tròn ceil
    salary_type: negotiable | range | upper_bound | unknown
    """
    if not s or s.strip() in ("Thoả thuận", "Thỏa thuận", ""):
        return (-1, -1, "VND", "negotiable")

    s = s.strip()

    # Detect currency và multiplier
    currency = "VND"
    rate = 1.0
    if "USD" in s or "$" in s:
        currency = "USD"
        rate = USD_RATE
    elif any(x in s for x in ("JPY", "¥", "Yen", "yen")):
        currency = "JPY"
        rate = JPY_RATE

    # Làm sạch số: bỏ ký hiệu tiền, dấu phẩy
    s_num = re.sub(r'[A-Za-z$¥,\s]', '', s)

    # Pattern "X - Y"
    m = re.search(r'(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)', s_num)
    if m:
        mn = math.ceil(float(m.group(1)) * rate)
        mx = math.ceil(float(m.group(2)) * rate)
        return (mn, mx, currency, "range")

    # Pattern "Tới X" / "Upto X" / "Up to X"
    m = re.search(r'(?:Tới|tới|Upto|upto|Up\s*to)\s*(\d+(?:\.\d+)?)', s)
    if m:
        mx = math.ceil(float(m.group(1)) * rate)
        mn = math.ceil(mx / 2)
        return (mn, mx, currency, "upper_bound")

    return (-1, -1, currency, "unknown")

salary_schema = StructType() \
    .add("min_salary",      IntegerType()) \
    .add("max_salary",      IntegerType()) \
    .add("salary_currency", StringType()) \
    .add("salary_type",     StringType())

parse_salary_udf = udf(parse_salary, salary_schema)

# ─────────────────────────────────────────
# 5. UDF: PARSE EXPERIENCE
# ─────────────────────────────────────────
def parse_experience(s: str):
    """
    Trả về (exp_min, exp_type)
    exp_min: Float, -1.0 nếu không xác định
    exp_type: no_requirement | less_than_1 | exact | unknown
    """
    if not s:
        return (-1.0, "unknown")
    s = s.strip()
    if s in ("Không yêu cầu", "Không yêu cầu kinh nghiệm"):
        return (0.0, "no_requirement")
    if re.search(r'[Dd]ưới\s*1', s):
        return (0.0, "less_than_1")
    m = re.search(r'(\d+(?:[.,]\d+)?)\s*năm', s)
    if m:
        val = float(m.group(1).replace(',', '.'))
        return (val, "exact")
    return (-1.0, "unknown")

exp_schema = StructType() \
    .add("exp_min",  FloatType()) \
    .add("exp_type", StringType())

parse_exp_udf = udf(parse_experience, exp_schema)

# ─────────────────────────────────────────
# 6. UDF: PARSE DEADLINE
# ─────────────────────────────────────────
def parse_deadline(s: str):
    if not s:
        return None
    s = re.sub(r'^.*?:\s*', '', s.strip())
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None

parse_deadline_udf = udf(parse_deadline, DateType())

# ─────────────────────────────────────────
# 7. LOCATION — chỉ strip artifact TopCV
# Normalize tên tỉnh giao LLM (File 3)
# ─────────────────────────────────────────
def strip_location(s: str) -> str:
    if not s:
        return ""
    return re.sub(r'\s*\(mới\)|\s*\(new\)', '', s, flags=re.IGNORECASE).strip()

strip_location_udf = udf(strip_location, StringType())

# ─────────────────────────────────────────
# 8. UDF: PARSE TAGS_REQUIREMENT
# ─────────────────────────────────────────
def parse_tags_req(tags: list):
    """
    Trích: edu_level, gender, age_min, age_max, req_languages
    """
    if not tags:
        return ("unknown", "any", -1, -1, [])

    edu, gender = "unknown", "any"
    age_min, age_max = -1, -1
    languages = []

    for tag in tags:
        t = tag.strip()

        # Học vấn
        if any(x in t for x in ["Đại Học", "Cao Đẳng", "Trung cấp", "Thạc sỹ", "Tiến sĩ"]):
            edu = t

        # Giới tính
        if t in ("Nam", "Nữ"):
            gender = t

        # Tuổi range: "Tuổi 22 - 35"
        m = re.search(r'Tuổi\s+(\d+)\s*[-–]\s*(\d+)', t)
        if m:
            age_min, age_max = int(m.group(1)), int(m.group(2))

        # Tuổi từ: "Từ 25 tuổi trở lên"
        m = re.search(r'Từ\s+(\d+)\s*tuổi', t)
        if m:
            age_min = int(m.group(1))

        # Ngôn ngữ
        m = re.match(r'(Tiếng \w+)', t)
        if m:
            languages.append(m.group(1))

    return (edu, gender, age_min, age_max, languages)

tags_req_schema = StructType() \
    .add("edu_level",     StringType()) \
    .add("gender",        StringType()) \
    .add("age_min",       IntegerType()) \
    .add("age_max",       IntegerType()) \
    .add("req_languages", ArrayType(StringType()))

parse_tags_req_udf = udf(parse_tags_req, tags_req_schema)

# ─────────────────────────────────────────
# 9. PRELIMINARY is_it_job (rule-based)
# ─────────────────────────────────────────
IT_TAGS = {
    "IT - Phần mềm", "IT - Phần cứng và máy tính",
    "Chuyên môn Công nghệ thông tin khác",
    # Development
    "Frontend Developer", "Backend Developer", "Fullstack Developer",
    "Mobile Developer", "Software Engineer", "Game Developer",
    "Embedded Developer", "Firmware Engineer", "Lập trình viên",
    # Data / AI / ML
    "Data Engineer", "Data Scientist", "Data Analyst",
    "Machine Learning Engineer", "AI Engineer", "MLOps Engineer",
    "BI Developer", "Business Intelligence",
    # Infrastructure / DevOps / Cloud
    "DevOps Engineer", "Site Reliability Engineer", "SRE",
    "Cloud Engineer", "System Administrator", "Quản trị hệ thống",
    "Network Engineer", "Kỹ sư mạng", "Infrastructure Engineer",
    # Security
    "An toàn thông tin", "An ninh mạng", "Cyber Security",
    "Bảo mật thông tin", "Quản trị và vận hành bảo mật",
    "Penetration Tester", "Security Engineer", "SOC Analyst",
    # Testing / QA
    "Software Tester (Automation & Manual)", "Automation Tester",
    "Manual Tester", "Performance Tester",
    "QA Engineer", "QC Engineer", "Tester",
    # Analysis / Management
    "Business Analyst (Phân tích nghiệp vụ)", "Business Analyst",
    "IT Project Manager", "IT Consultant", "IT Business Analyst",
    "Product Manager", "Product Owner", "Scrum Master", "Agile Coach",
    # Bridge / Communication
    "Kỹ sư cầu nối BrSE", "BrSE", "IT Comtor", "Comtor",
    "Delivery Manager",
    # Support / Helpdesk
    "Helpdesk", "IT Support", "Hỗ trợ kỹ thuật IT",
    "Technical Support", "IT Helpdesk",
    # Database
    "Database Administrator", "DBA", "Database Engineer",
    # UI/UX (sản phẩm phần mềm)
    "UI/UX Designer", "UX Designer", "UI Designer", "UX Researcher",
    # ERP / Enterprise
    "ERP Consultant", "SAP Consultant", "Salesforce Developer",
    # Blockchain
    "Blockchain Developer", "Smart Contract Developer",
}

NON_IT_TAGS = {
    "Thiết kế đồ họa (Graphic Design)", "Illustration",
    "Thiết kế / Kiến trúc", "In ấn / Xuất bản",
    "Mỹ thuật / Nghệ thuật / Điện ảnh",
    "Cơ khí / Tự động hóa", "Sản xuất",
    "Giáo dục / Đào tạo", "Giáo viên Tin học",
    "Kinh doanh phần mềm", "Online Sales", "Telesales",
    "B2B", "B2G", "Marketing / Quảng cáo",
}

# Tags vùng xám → is_it_preliminary = "none" → LLM quyết
# VD: "Ngân hàng", "Tài chính", "Kỹ sư lập trình PLC/SCADA"
# Không cần liệt kê — chúng sẽ không match IT_TAGS lẫn NON_IT_TAGS

def classify_it(tags: list) -> str:
    if not tags:
        return "none"
    t = set(tags)
    if t & IT_TAGS:     return "true"
    if t & NON_IT_TAGS: return "false"
    return "none"

classify_it_udf = udf(classify_it, StringType())

# ─────────────────────────────────────────
# 10. ĐỌC TỪ HDFS
# ─────────────────────────────────────────
df = spark.read \
    .schema(schema) \
    .parquet("hdfs://namenode:9000/datalake/silver/topcv/stage_unique/")

df = df.filter(col("job_id").isNotNull())
print(f"📥 Đọc được {df.count()} jobs từ stage_unique")

# ─────────────────────────────────────────
# 11. CLEAN HTML → TEXT
# ─────────────────────────────────────────
df = df \
    .withColumn("mo_ta_text",     clean_html_udf(col("mo_ta_cong_viec_html"))) \
    .withColumn("yeu_cau_text",   clean_html_udf(col("yeu_cau_ung_vien_html"))) \
    .withColumn("quyen_loi_text", clean_html_udf(col("quyen_loi_html"))) \
    .drop("mo_ta_cong_viec_html", "yeu_cau_ung_vien_html", "quyen_loi_html")

# ─────────────────────────────────────────
# 12. FLAG: has_content
# Job không có nội dung JD → skip Ollama ở File 3
# ─────────────────────────────────────────
df = df.withColumn(
    "has_content",
    when(
        (length(col("mo_ta_text")) > 50) |
        (length(col("yeu_cau_text")) > 50),
        True
    ).otherwise(False)
)

# ─────────────────────────────────────────
# 13. PARSE SALARY
# ─────────────────────────────────────────
df = df \
    .withColumn("salary_raw",      col("salary")) \
    .withColumn("_sal",            parse_salary_udf(col("salary"))) \
    .withColumn("min_salary",      col("_sal.min_salary")) \
    .withColumn("max_salary",      col("_sal.max_salary")) \
    .withColumn("salary_currency", col("_sal.salary_currency")) \
    .withColumn("salary_type",     col("_sal.salary_type")) \
    .drop("_sal", "salary")

# ─────────────────────────────────────────
# 14. PARSE EXPERIENCE
# ─────────────────────────────────────────
df = df \
    .withColumn("_exp",     parse_exp_udf(col("experience"))) \
    .withColumn("exp_min",  col("_exp.exp_min")) \
    .withColumn("exp_type", col("_exp.exp_type")) \
    .drop("_exp", "experience")

# ─────────────────────────────────────────
# 15. PARSE DEADLINE
# ─────────────────────────────────────────
df = df \
    .withColumn("deadline_date", parse_deadline_udf(col("deadline"))) \
    .drop("deadline")

# ─────────────────────────────────────────
# 16. LOCATION — strip artifact, giữ raw cho LLM
# ─────────────────────────────────────────
df = df \
    .withColumn("location_raw", strip_location_udf(col("location"))) \
    .drop("location")

# ─────────────────────────────────────────
# 17. PARSE TAGS_REQUIREMENT
# ─────────────────────────────────────────
df = df \
    .withColumn("_treq",          parse_tags_req_udf(col("tags_requirement"))) \
    .withColumn("edu_level",      col("_treq.edu_level")) \
    .withColumn("gender",         col("_treq.gender")) \
    .withColumn("age_min",        col("_treq.age_min")) \
    .withColumn("age_max",        col("_treq.age_max")) \
    .withColumn("req_languages",  col("_treq.req_languages")) \
    .drop("_treq", "tags_requirement")

# ─────────────────────────────────────────
# 18. PRELIMINARY is_it_job
# ─────────────────────────────────────────
df = df.withColumn(
    "is_it_preliminary",
    classify_it_udf(col("tags_specialization"))
)

# ─────────────────────────────────────────
# 19. DATA QUALITY REPORT
# ─────────────────────────────────────────
total         = df.count()
no_content    = df.filter(col("has_content") == False).count()
null_salary   = df.filter(col("min_salary") == -1).count()
null_exp      = df.filter(col("exp_min") == -1).count()
null_deadline = df.filter(col("deadline_date").isNull()).count()
it_true       = df.filter(col("is_it_preliminary") == "true").count()
it_false      = df.filter(col("is_it_preliminary") == "false").count()
it_none       = df.filter(col("is_it_preliminary") == "none").count()

print(f"\n📊 DATA QUALITY REPORT — stage_clean")
print(f"   Tổng jobs              : {total}")
print(f"   Không có nội dung JD   : {no_content} ({no_content/total:.1%})")
print(f"   Salary không xác định  : {null_salary} ({null_salary/total:.1%})")
print(f"   Experience không rõ    : {null_exp} ({null_exp/total:.1%})")
print(f"   Deadline null          : {null_deadline} ({null_deadline/total:.1%})")
print(f"   is_it = true  (rule)   : {it_true} ({it_true/total:.1%})")
print(f"   is_it = false (rule)   : {it_false} ({it_false/total:.1%})")
print(f"   is_it = none  (→ LLM)  : {it_none} ({it_none/total:.1%})")
print(f"   → LLM sẽ xử lý        : {it_none + no_content} jobs")

# ─────────────────────────────────────────
# 20. GHI RA HDFS
# ─────────────────────────────────────────
CLEAN_PATH = "hdfs://namenode:9000/datalake/silver/topcv/stage_clean/"

df.write \
    .mode("overwrite") \
    .parquet(CLEAN_PATH)

print(f"\n✅ Xong! Ghi tại: {CLEAN_PATH}")
df.printSchema()