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
BATCH_SIZE      = 120
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
    .config("spark.network.timeout",            "800s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .config("spark.sql.shuffle.partitions",     str(NUM_PARTITIONS)) \
    .config("spark.executor.memory",            "1800m") \
    .config("spark.driver.memory",              "1g") \
    .config("spark.speculation",                "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ═══════════════════════════════════════════════════════════
# 2. JOB_LEVEL — rule Python thuần, không dùng LLM (giữ nguyên)
# ═══════════════════════════════════════════════════════════
def get_level_from_title(title: str):
    if not title:
        return None
    t = title.lower()
    if any(x in t for x in ["senior", "sr.", " sr "]):              return "Senior"
    if any(x in t for x in ["junior", "jr.", " jr "]):              return "Junior"
    if "middle" in t or " mid " in t:                               return "Middle"
    if any(x in t for x in ["lead", "tech lead"]):                  return "Lead"
    if any(x in t for x in ["manager", "quản lí", "quản lý"]):     return "Manager"
    if any(x in t for x in ["intern", "thực tập", "thực tập sinh"]): return "Intern"
    return None

def get_level_from_exp(exp_min) -> str:
    if exp_min is None or exp_min < 0: return "Unknown"
    if exp_min >= 7:  return "Senior"
    if exp_min >= 3:  return "Middle"
    return "Junior"

def resolve_job_level(title, exp_min) -> str:
    return get_level_from_title(title) or get_level_from_exp(exp_min)

# ═══════════════════════════════════════════════════════════
# 3. PROMPT
# ═══════════════════════════════════════════════════════════
PROMPT_TEMPLATE = """Bạn là chuyên gia phân tích JD IT Việt Nam.
Trả về JSON ONLY, không markdown, không giải thích.

=== INPUT ===
Tiêu đề: {title}
Tags chuyên môn: {tags_specialization}
salary_type: {salary_type} | min_salary: {min_salary} | max_salary: {max_salary}
exp_min: {exp_min} | exp_type: {exp_type}
is_it_preliminary: {is_it_preliminary}
location_raw: {location_raw}
Mô tả: {mo_ta}
Yêu cầu: {yeu_cau}
Quyền lợi: {quyen_loi}

=== NHIỆM VỤ ===

1. DOMAIN — chọn 1 lĩnh vực liên quan cao nhất:
   AI Scientist | AI Engineer | Data Engineer | Data Scientist |
   DevOps | Cyber Security | Tester/QA | Business Analyst |
   Backend Developer | Frontend Developer | Mobile Developer |
   Web Fullstack Developer | IoT | Game Developer |
   Leader | Manager | BrSE | Other

2. TECHNICAL_SKILLS
   Trích xuất kỹ năng kỹ thuật từ MÔ TẢ + YÊU CẦU, KHÔNG lấy từ tags.
   Mỗi skill là 1 string riêng biệt trong array.

   PHÂN LOẠI — chỉ lấy các nhóm sau:
   - Ngôn ngữ lập trình : Python, Java, C#, C++, Go, Kotlin, Swift, PHP, Ruby, Scala, R, Rust...
   - Framework/Library  : Spring Boot, Django, FastAPI, React, Vue, Angular, Flutter, Express...
   - Database           : MySQL, PostgreSQL, MongoDB, Redis, Elasticsearch, Oracle, Cassandra...
   - Cloud/DevOps       : AWS, GCP, Azure, Docker, Kubernetes, Terraform, Jenkins, Ansible...
   - Công cụ            : Git, Jira, Kafka, RabbitMQ, Airflow, Spark, Hadoop, Postman...
   - Giao thức/Chuẩn    : REST API, GraphQL, gRPC, WebSocket, OAuth, JWT, MQTT...

   KHÔNG lấy:
   - Soft skills        : "giao tiếp", "teamwork", "chịu áp lực"
   - Kinh nghiệm chung  : "có kinh nghiệm làm việc", "hiểu biết về..."
   - Trình độ học vấn   : "đại học", "cao đẳng"
   - Phương pháp chung  : "Agile", "Scrum", "Kanban"

   CHUẨN HÓA THEO DOMAIN:

   ── AI Scientist / AI Engineer ──────────────────────────────
   - Python: "python","python3" → "Python"
   - TensorFlow: "tensorflow","tf" → "TensorFlow"
   - PyTorch: "pytorch","torch" → "PyTorch"
   - Scikit-learn: "sklearn","scikit learn" → "Scikit-learn"
   - Keras: "keras" → "Keras"
   - Hugging Face: "huggingface","hf","transformers" → "Hugging Face"
   - LangChain: "langchain","lang chain" → "LangChain"
   - OpenCV: "opencv","open cv" → "OpenCV"
   - CUDA: "cuda" → "CUDA"
   - LLM: "llm","large language model" → "LLM"
   - RAG: "rag","retrieval augmented" → "RAG"
   - Pandas: "pandas" → "Pandas"
   - NumPy: "numpy","np" → "NumPy"

   ── Data Engineer / Data Scientist ──────────────────────────
   - Spark: "spark","pyspark","apache spark" → "Spark"
   - Kafka: "kafka","apache kafka" → "Kafka"
   - Airflow: "airflow","apache airflow" → "Airflow"
   - dbt: "dbt","data build tool" → "dbt"
   - Hadoop: "hadoop","hdfs" → "Hadoop"
   - Hive: "hive","apache hive" → "Hive"
   - Databricks: "databricks" → "Databricks"
   - Snowflake: "snowflake" → "Snowflake"
   - BigQuery: "bigquery","big query" → "BigQuery"
   - Redshift: "redshift","aws redshift" → "Redshift"
   - Power BI: "powerbi","power bi" → "Power BI"
   - Tableau: "tableau" → "Tableau"
   - Looker: "looker","looker studio" → "Looker"
   - SQL: "sql","hive sql","spark sql" → "SQL"
   - Pandas: "pandas" → "Pandas"
   - NumPy: "numpy" → "NumPy"

   ── Backend Developer ────────────────────────────────────────
   - Spring Boot: "springboot","spring-boot","spring framework" → "Spring Boot"
   - Django: "django" → "Django"
   - FastAPI: "fastapi","fast api" → "FastAPI"
   - Flask: "flask" → "Flask"
   - Express.js: "express","expressjs","express.js" → "Express.js"
   - NestJS: "nestjs","nest.js","nest js" → "NestJS"
   - Laravel: "laravel" → "Laravel"
   - Node.js: "nodejs","node.js","node js" → "Node.js"
   - gRPC: "grpc" → "gRPC"
   - REST API: "rest","restful","rest api" → "REST API"
   - GraphQL: "graphql","graph ql" → "GraphQL"
   - RabbitMQ: "rabbitmq","rabbit mq" → "RabbitMQ"
   - Redis: "redis" → "Redis"

   ── Frontend Developer ───────────────────────────────────────
   - React: "ReactJS","React.js","reactjs" → "React"
   - Vue: "VueJS","Vue.js","vuejs" → "Vue"
   - Angular: "angular","angularjs" → "Angular"
   - Next.js: "nextjs","next.js","next js" → "Next.js"
   - Nuxt.js: "nuxtjs","nuxt.js" → "Nuxt.js"
   - TypeScript: "TS","typescript" → "TypeScript"
   - JavaScript: "JS","javascript","Javascript" → "JavaScript"
   - HTML/CSS: "html","css","html5","css3" → "HTML/CSS"
   - Tailwind CSS: "tailwind","tailwindcss" → "Tailwind CSS"
   - Webpack: "webpack" → "Webpack"
   - Vite: "vite" → "Vite"
   - Redux: "redux","redux toolkit" → "Redux"

   ── Mobile Developer ─────────────────────────────────────────
   - Flutter: "flutter" → "Flutter"
   - Dart: "dart" → "Dart"
   - React Native: "react native","reactnative" → "React Native"
   - Swift: "swift" → "Swift"
   - Kotlin: "kotlin" → "Kotlin"
   - iOS: "ios","IOS","iphone os" → "iOS"
   - Android: "android" → "Android"
   - Xcode: "xcode" → "Xcode"
   - Android Studio: "android studio" → "Android Studio"
   - Firebase: "firebase" → "Firebase"

   ── Web Fullstack Developer ──────────────────────────────────
   - Kết hợp Backend + Frontend
   - MEAN/MERN: ghi riêng từng thành phần (MongoDB, Express, React, Node.js)

   ── DevOps ───────────────────────────────────────────────────
   - Docker: "docker","Docker CE" → "Docker"
   - Kubernetes: "k8s","kubernetes" → "Kubernetes"
   - Terraform: "terraform" → "Terraform"
   - Ansible: "ansible" → "Ansible"
   - Jenkins: "jenkins" → "Jenkins"
   - GitLab CI: "gitlab ci","gitlab ci/cd" → "GitLab CI"
   - GitHub Actions: "github actions" → "GitHub Actions"
   - Helm: "helm" → "Helm"
   - Prometheus: "prometheus" → "Prometheus"
   - Grafana: "grafana" → "Grafana"
   - AWS: "amazon web services","aws" → "AWS"
   - GCP: "google cloud","gcp" → "GCP"
   - Azure: "microsoft azure","azure" → "Azure"
   - CI/CD: "cicd","ci-cd","ci/cd" → "CI/CD"
   - Linux: "linux","ubuntu","centos" → "Linux"
   - Nginx: "nginx" → "Nginx"

   ── Cyber Security ───────────────────────────────────────────
   - Penetration Testing: "pentest","penetration test" → "Penetration Testing"
   - SIEM: "siem","splunk","qradar" → "SIEM"
   - Burp Suite: "burp suite","burpsuite" → "Burp Suite"
   - Metasploit: "metasploit" → "Metasploit"
   - Wireshark: "wireshark" → "Wireshark"
   - Nmap: "nmap" → "Nmap"
   - OWASP: "owasp" → "OWASP"
   - Firewall: "firewall","waf" → "Firewall"
   - IDS/IPS: "ids","ips","intrusion detection" → "IDS/IPS"
   - SSL/TLS: "ssl","tls","pki" → "SSL/TLS"
   - ISO 27001: "iso 27001","iso27001" → "ISO 27001"

   ── Tester / QA ──────────────────────────────────────────────
   - Selenium: "selenium" → "Selenium"
   - Cypress: "cypress" → "Cypress"
   - Appium: "appium" → "Appium"
   - JMeter: "jmeter","apache jmeter" → "JMeter"
   - Postman: "postman" → "Postman"
   - TestNG: "testng" → "TestNG"
   - Jest: "jest" → "Jest"
   - Playwright: "playwright" → "Playwright"
   - LoadRunner: "loadrunner","load runner" → "LoadRunner"
   - SQL: "sql" → "SQL"

   ── Business Analyst ─────────────────────────────────────────
   - SQL: "sql" → "SQL"
   - Power BI: "powerbi","power bi" → "Power BI"
   - Tableau: "tableau" → "Tableau"
   - Figma: "figma" → "Figma"
   - BPMN: "bpmn","business process" → "BPMN"
   - UML: "uml" → "UML"
   - Jira: "jira" → "Jira"
   - Confluence: "confluence" → "Confluence"
   - Excel: "excel","microsoft excel" → "Excel"

   ── IoT ──────────────────────────────────────────────────────
   - MQTT: "mqtt" → "MQTT"
   - Arduino: "arduino" → "Arduino"
   - Raspberry Pi: "raspberry pi","raspi" → "Raspberry Pi"
   - Embedded C: "embedded c","c embedded" → "Embedded C"
   - RTOS: "rtos","freertos" → "RTOS"
   - Zigbee: "zigbee" → "Zigbee"
   - LoRaWAN: "lorawan","lora" → "LoRaWAN"

   ── Game Developer ───────────────────────────────────────────
   - Unity: "unity","unity3d" → "Unity"
   - Unreal Engine: "unreal","ue4","ue5" → "Unreal Engine"
   - C#: "c#","csharp" → "C#"
   - C++: "c++","cpp" → "C++"
   - Blender: "blender" → "Blender"
   - Shader: "shader","hlsl","glsl" → "Shader"

   ── CHUẨN HÓA CHUNG ──────────────────────────────────────────
   - PostgreSQL: "postgres","postgresql" → "PostgreSQL"
   - MongoDB: "mongo","mongodb","Mongo DB" → "MongoDB"
   - MySQL: "mysql","My SQL" → "MySQL"
   - Git: "git","GIT" → "Git"
   - Linux: "linux","ubuntu","centos","debian" → "Linux"
   - Java: "java" → "Java"
   - Python: "python","python3" → "Python"
   - Quy tắc: giữ cách viết chuẩn nhà phát hành
     (React không phải REACT, iOS không phải IOS)

3. IS_IT_JOB
   - false nếu domain = "Other"
   - true cho tất cả domain còn lại
   - Nếu is_it_preliminary = "true"/"false" → ưu tiên giữ nguyên

4. EXP_MAX
   - Giới hạn trên kinh nghiệm, VD "3-5 năm" → 5.0
   - Không có → -1.0

5. LOCATIONS
   - Tên tỉnh/thành chuẩn hành chính từ location_raw
   - Bỏ: "nơi khác", số đếm, "remote"
   - Không xác định được → ["Toàn quốc"]

6. MAX_SALARY_VERIFIED
   - Chỉ khi salary_type = "negotiable"
   - Số lương cụ thể trong quyền lợi → số triệu VND
   - Không có → -1

=== OUTPUT ===
{{"domain":"Backend Developer","technical_skills":[],"is_it_job":true,"exp_max":-1.0,"locations":[],"max_salary_verified":-1}}"""

# ═══════════════════════════════════════════════════════════
# 4. OLLAMA CALLER
# [SỬA] Thêm tham số tags_specialization
# [SỬA] Sửa DEFAULT domain "AI Engineer" → "Other"
# [SỬA] Tăng num_predict 256 → 512
# [SỬA] Thêm tags_specialization vào PROMPT_TEMPLATE.format()
# ═══════════════════════════════════════════════════════════
def call_ollama(title, mo_ta, yeu_cau, quyen_loi,
                salary_type, min_salary, max_salary,
                exp_min, exp_type,
                is_it_preliminary, location_raw,
                tags_specialization) -> dict:          # [SỬA] thêm param

    DEFAULT = {
        "domain":              "Other",                # [SỬA] "AI Engineer" → "Other"
        "technical_skills":    [],
        "is_it_job":           False,
        "exp_max":             -1.0,
        "locations":           [],
        "max_salary_verified": -1,
    }

    prompt = PROMPT_TEMPLATE.format(
        title               = title             or "",
        tags_specialization = str(tags_specialization or []),  # [SỬA] thêm
        salary_type         = salary_type       or "unknown",
        min_salary          = min_salary        if min_salary != -1   else "không rõ",
        max_salary          = max_salary        if max_salary != -1   else "không rõ",
        exp_min             = exp_min           if exp_min   != -1.0  else "không rõ",
        exp_type            = exp_type          or "unknown",
        is_it_preliminary   = is_it_preliminary or "none",
        location_raw        = location_raw      or "",
        mo_ta               = (mo_ta     or "")[:600],
        yeu_cau             = (yeu_cau   or "")[:600],
        quyen_loi           = (quyen_loi or "")[:300],
    )

    for attempt in range(2):
        try:
            resp = requests.post(
                OLLAMA_URL,
                json={
                    "model":  OLLAMA_MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.1,
                        "num_predict": 512,            # [SỬA] 256 → 512
                    },
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
# 5. SCHEMA
# [SỬA] Thêm "domain" vào đầu enrich_schema
# ═══════════════════════════════════════════════════════════
enrich_schema = StructType() \
    .add("domain",              StringType()) \
    .add("technical_skills",    ArrayType(StringType())) \
    .add("is_it_job",           BooleanType()) \
    .add("exp_max",             FloatType()) \
    .add("locations",           ArrayType(StringType())) \
    .add("max_salary_verified", IntegerType())

# ═══════════════════════════════════════════════════════════
# 6. UDF ENRICH
# [SỬA] Thêm tham số tags_specialization
# [SỬA] Thêm domain vào return tuple
# [SỬA] is_it dùng domain thay vì is_it_job từ LLM
# ═══════════════════════════════════════════════════════════
def enrich_job(title, mo_ta, yeu_cau, quyen_loi,
               salary_type, min_salary, max_salary,
               exp_min, exp_type,
               is_it_preliminary, location_raw,
               tags_specialization):                     # [SỬA] thêm param

    r = call_ollama(
        title, mo_ta, yeu_cau, quyen_loi,
        salary_type, min_salary, max_salary,
        exp_min, exp_type,
        is_it_preliminary, location_raw,
        tags_specialization                              # [SỬA] truyền vào
    )

    domain = r.get("domain", "Other")                   # [SỬA] lấy domain

    # [SỬA] dùng domain để quyết định is_it, nhất quán với prompt
    if is_it_preliminary == "true":    is_it = True
    elif is_it_preliminary == "false": is_it = False
    else:                              is_it = (domain != "Other")  # [SỬA]

    return (
        domain,                                          # [SỬA] thêm vào tuple
        r.get("technical_skills",    []),
        is_it,
        float(r.get("exp_max",       -1.0)),
        r.get("locations",           []),
        int(r.get("max_salary_verified", -1) or -1),
    )

enrich_udf = udf(enrich_job, enrich_schema)

job_level_udf = udf(
    lambda title, exp_min: resolve_job_level(title, exp_min),
    StringType()
)

# ═══════════════════════════════════════════════════════════
# 7. ĐỌC DATA (giữ nguyên)
# ═══════════════════════════════════════════════════════════
df = spark.read.parquet(STAGE_PATH)

df_no_content  = df.filter(col("has_content") == False).cache()
df_has_content = df.filter(col("has_content") == True).cache()

skip_no_content = df_no_content.count()
print(f" Tổng jobs đọc vào  : {df.count()}")
print(f" Skip (no content)  : {skip_no_content}")

df_non_it    = df_has_content.filter(col("is_it_preliminary") == "false").cache()
df_to_enrich = df_has_content.filter(col("is_it_preliminary") != "false").cache()

skip_non_it  = df_non_it.count()
total_enrich = df_to_enrich.count()
print(f" Skip (non-IT rule) : {skip_non_it}")
print(f" Gọi Ollama cho     : {total_enrich} jobs")

# ═══════════════════════════════════════════════════════════
# 8. RESUME (giữ nguyên)
# ═══════════════════════════════════════════════════════════
def get_done_batches() -> set:
    try:
        done_df = spark.read.parquet(CHECKPOINT_PATH)
        done = {int(r["_batch_id"])
                for r in done_df.select("_batch_id").distinct().collect()}
        print(f" Resume: {len(done)} batch đã xong → {sorted(done)}")
        return done
    except Exception:
        print(" Chưa có checkpoint — bắt đầu từ batch 0")
        return set()

done_batches = get_done_batches()

# ═══════════════════════════════════════════════════════════
# 9. CHẠY TỪNG BATCH
# [SỬA] Thêm col("tags_specialization") vào enrich_udf call
# [SỬA] Thêm withColumn("domain", ...) vào df_batch_enriched
# ═══════════════════════════════════════════════════════════
rows        = df_to_enrich.collect()
num_batches = math.ceil(total_enrich / BATCH_SIZE)

print(f"\nBatch size: {BATCH_SIZE} | Số batch: {num_batches} | Partitions: {NUM_PARTITIONS}")
print("─" * 60)

for batch_idx in range(num_batches):

    if batch_idx in done_batches:
        start = batch_idx * BATCH_SIZE
        end   = min(start + BATCH_SIZE, total_enrich)
        print(f"⏭ Batch {batch_idx+1:>2}/{num_batches} ({end-start} jobs) — bỏ qua")
        continue

    start      = batch_idx * BATCH_SIZE
    end        = min(start + BATCH_SIZE, total_enrich)
    batch_rows = rows[start:end]

    print(f"🔄 Batch {batch_idx+1:>2}/{num_batches} ({len(batch_rows)} jobs) ...")

    df_batch = spark.createDataFrame(batch_rows, schema=df_to_enrich.schema) \
                    .repartition(NUM_PARTITIONS)

    df_batch_enriched = (
        df_batch
        .withColumn(
            "_enriched",
            enrich_udf(
                col("title"), col("mo_ta_text"), col("yeu_cau_text"),
                col("quyen_loi_text"), col("salary_type"),
                col("min_salary"), col("max_salary"),
                col("exp_min"), col("exp_type"),
                col("is_it_preliminary"), col("location_raw"),
                col("tags_specialization"),
            )
        )
        # [SỬA] thêm
        .withColumn("domain", col("_enriched.domain"))
        .withColumn("technical_skills", col("_enriched.technical_skills"))
        .withColumn("is_it_job", col("_enriched.is_it_job"))
        .withColumn("exp_max", col("_enriched.exp_max"))
        .withColumn("locations", col("_enriched.locations"))
        .withColumn("max_salary_verified", col("_enriched.max_salary_verified"))
        .withColumn("job_level", job_level_udf(col("title"), col("exp_min")))
        .withColumn(
            "classification_source",
            when(col("is_it_preliminary") == "none", lit("llm"))
            .otherwise(lit("rule"))
        )
        .withColumn("_batch_id", lit(batch_idx))
        .drop("_enriched")
    )

    df_batch_enriched.write \
        .mode("append") \
        .parquet(CHECKPOINT_PATH)

    pct = (batch_idx + 1) / num_batches * 100
    print(f"   ✅ Xong | {batch_idx+1}/{num_batches} ({pct:.0f}%)")

print("\n" + "─" * 60)
print("📦 Gộp checkpoint → ghi silver/final ...")

# ═══════════════════════════════════════════════════════════
# 10. GỘP CHECKPOINT → FILTER IT → GHI FINAL (giữ nguyên)
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
# [SỬA] Thêm thống kê phân bố domain
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

# [SỬA] Thêm thống kê domain để kiểm tra LLM classify đúng không
print(f"\n   Phân bố domain:")
df_it.groupBy("domain").count().orderBy(col("count").desc()).show(20, truncate=False)

if unk_level / max(total_it, 1) > 0.3:
    print("⚠ CẢNH BÁO: >30% job_level Unknown")
if empty_sk / max(total_it, 1) > 0.2:
    print("⚠ CẢNH BÁO: >20% skills rỗng")

# [SỬA] Thêm cảnh báo nếu domain Other quá nhiều
other_count = df_it.filter(col("domain") == "Other").count()
if other_count / max(total_it, 1) > 0.15:
    print(f"⚠ CẢNH BÁO: >15% domain=Other ({other_count}) — kiểm tra lại prompt")

print(f"\n✅ Xong! Silver final: {SILVER_PATH}")
df_it.printSchema()