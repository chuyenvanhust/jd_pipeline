# TopCV IT Job Pipeline

Hệ thống thu thập, xử lý và phân tích dữ liệu tuyển dụng IT từ TopCV theo mô hình **Medallion Architecture** (Bronze → Silver → Gold).

---

## Kiến trúc tổng quan

```
Crawler (Python)
    └─► Bronze: local storage (JSON)
            └─► Kafka topic: job_raw
                    └─► Spark Silver: HDFS Parquet
                            └─► Spark Gold: HDFS Parquet
                                    └─► Hive Tables
                                            └─► FastAPI + UI (TODO)
```

### Stack công nghệ

| Service | Image | Vai trò | Port |
|---|---|---|---|
| Zookeeper | confluentinc/cp-zookeeper:7.5.0 | Quản lý Kafka | 2181 |
| Kafka | confluentinc/cp-kafka:7.5.0 | Message broker | 9092, 29092 |
| Kafka UI | provectuslabs/kafka-ui | Dashboard Kafka | 8090 |
| Spark Master | apache/spark:3.5.3 | Nhận & phân task | 8080, 7077 |
| Spark Worker x2 | apache/spark:3.5.3 | Thực thi xử lý | - |
| NameNode | bde2020/hadoop-namenode | Metadata HDFS | 50070, 9000 |
| DataNode | bde2020/hadoop-datanode | Lưu data HDFS | 50075 |
| Hive Server | bde2020/hive:2.3.2 | SQL trên HDFS | 10000, 10002 |
| PostgreSQL | postgres:12-alpine | Hive Metastore | 5432 |
| Ollama | ollama/ollama | LLM local (GPU) | 11434 |

---

## Cấu trúc thư mục

```
topcv-it-job-pipeline/
├── docker/
│   └── docker-compose.yml
├── ingestion/
│   ├── crawler.py              # Crawl TopCV → local JSON
│   └── kafka_producer.py       # Đẩy JSON vào Kafka
├── processing/
│   ├── silver/
│   │   ├── transform_unique.py # Dedup by job_id
│   │   ├── transform_clean.py  # Strip HTML
│   │   ├── transform_enrich.py # LLM enrichment (Ollama)
│   │   ├── silver_table.hql    # Tạo Hive Silver table
│   │   └── check_ckpt.py       # Kiểm tra checkpoint
│   └── gold/
│       ├── agg_skills.py       # Top skills demand
│       ├── agg_salary.py       # Salary benchmark
│       ├── agg_location.py     # Location trend
│       └── gold_tables.hql     # Tạo Hive Gold tables
├── requirements.txt
└── .env
```

---

## Hướng dẫn chạy từ đầu đến cuối

### Bước 0 — Yêu cầu

- Docker Desktop đang chạy
- GPU NVIDIA (cho Ollama) — hoặc bỏ phần `deploy.resources` trong docker-compose nếu dùng CPU
- Python 3.10+ và virtualenv (cho crawler và kafka producer)

---

### Bước 1 — Khởi động toàn bộ stack

```bash
cd docker
docker compose up -d
```

Kiểm tra tất cả container đang chạy:

```bash
docker compose ps
```

Chờ khoảng 30-60 giây để tất cả service healthy, sau đó kiểm tra:

| UI | URL |
|---|---|
| Spark Master | http://localhost:8080 |
| HDFS NameNode | http://localhost:50070 |
| Kafka UI | http://localhost:8090 |

---

### Bước 2 — Pull model LLM (chỉ cần làm 1 lần)

```bash
docker exec ollama ollama pull llama3.2:3b
```

Kiểm tra model đã tải:

```bash
docker exec ollama ollama list
```

---

### Bước 3 — Tạo thư mục HDFS và cấp quyền

```bash
docker exec namenode hdfs dfs -mkdir -p /datalake/silver/topcv
docker exec namenode hdfs dfs -mkdir -p /datalake/gold/reports
docker exec namenode hdfs dfs -chmod 777 /tmp
docker exec namenode hdfs dfs -chmod -R 777 /datalake
```

---

### Bước 4 — Bronze: Crawl data từ TopCV

Cài dependencies (chỉ lần đầu):

```bash
pip install -r requirements.txt
```

Chạy crawler:

```bash
python ingestion/crawler.py
```

Data sẽ được lưu tại `docker/storage/data_lake/bronze/topcv/<timestamp>/`.

---

### Bước 5 — Kafka: Đẩy Bronze data vào topic

Tạo Kafka topic (chỉ lần đầu):

```bash
docker exec kafka kafka-topics --create --topic job_raw --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
```

Chạy producer:

```bash
python ingestion/kafka_producer.py
```

Kiểm tra messages tại http://localhost:8090 → topic `job_raw`.

---

### Bước 6 — Silver: Xử lý dữ liệu bằng Spark

Copy các script vào Spark Master container:

```bash
docker exec spark-master mkdir -p /opt/spark/jobs

docker cp processing/silver/transform_unique.py spark-master:/opt/spark/jobs/transform_unique.py
docker cp processing/silver/transform_clean.py spark-master:/opt/spark/jobs/transform_clean.py
docker cp processing/silver/transform_enrich.py spark-master:/opt/spark/jobs/transform_enrich.py
```

Cài thư viện requests vào Spark workers:

```bash
docker exec -u root spark-master pip install requests
docker exec -u root spark-worker-1 pip install requests
docker exec -u root spark-worker-2 pip install requests
```

**Bước 6a — Deduplication:**

```bash
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --executor-memory 1800m --total-executor-cores 4 /opt/spark/jobs/transform_unique.py
```

**Bước 6b — Clean HTML:**

```bash
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --executor-memory 1800m --total-executor-cores 4 /opt/spark/jobs/transform_clean.py
```

**Bước 6c — LLM Enrichment** (mất ~1 giờ cho ~1800 jobs):

```bash
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --executor-memory 1800m --total-executor-cores 4 /opt/spark/jobs/transform_enrich.py
```

> Nếu bị kill giữa chừng, chạy lại lệnh trên — code tự resume từ batch đã xong.

Kiểm tra kết quả Silver trên HDFS:

```bash
docker exec namenode hdfs dfs -ls /datalake/silver/topcv/final/
```

---

### Bước 7 — Tạo Hive Silver Table

```bash
docker cp processing/silver/silver_table.hql hive-server:/silver_table.hql
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /silver_table.hql
```

Kiểm tra:

```bash
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "USE topcv_db; SELECT COUNT(*) FROM topcv_silver;"
```

---

### Bước 8 — Gold: Tổng hợp dữ liệu phân tích

Copy scripts:

```bash
docker cp processing/gold/agg_skills.py spark-master:/opt/spark/jobs/agg_skills.py
docker cp processing/gold/agg_salary.py spark-master:/opt/spark/jobs/agg_salary.py
docker cp processing/gold/agg_location.py spark-master:/opt/spark/jobs/agg_location.py
```

Tạo thư mục Gold trên HDFS:

```bash
docker exec namenode hdfs dfs -mkdir -p /datalake/gold/reports
docker exec namenode hdfs dfs -chmod 777 /datalake/gold/reports
```

Chạy từng file:

```bash
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --executor-memory 1800m --total-executor-cores 4 /opt/spark/jobs/agg_skills.py

docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --executor-memory 1800m --total-executor-cores 4 /opt/spark/jobs/agg_salary.py

docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --executor-memory 1800m --total-executor-cores 4 /opt/spark/jobs/agg_location.py
```

---

### Bước 9 — Tạo Hive Gold Tables

```bash
docker cp processing/gold/gold_tables.hql hive-server:/gold_tables.hql
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /gold_tables.hql
```

Kiểm tra toàn bộ tables:

```bash
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "USE topcv_db; SHOW TABLES;"
```

Kết quả mong đợi:

```
topcv_silver
dim_skills_demand
fact_salary_benchmark
dim_location_trend
```

---

## Lệnh tiện ích thường dùng

### Khởi động / Dừng

```bash
# Khởi động toàn bộ
docker compose up -d

# Dừng toàn bộ (giữ data)
docker compose stop

# Xóa toàn bộ (mất data volume)
docker compose down -v
```

### Kiểm tra trạng thái

```bash
# Xem tài nguyên đang dùng
docker stats --no-stream

# Xem log một service
docker logs -f spark-worker-1

# Xem files trên HDFS
docker exec namenode hdfs dfs -ls /datalake/
docker exec namenode hdfs dfs -du -h /datalake/
```

### Restart một service

```bash
docker restart spark-worker-1
docker restart spark-worker-2
docker restart ollama
```

### Query Hive nhanh

```bash
# Đếm IT jobs
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "USE topcv_db; SELECT COUNT(*) FROM topcv_silver;"

# Top 10 skills
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "USE topcv_db; SELECT skill_name, job_count FROM dim_skills_demand ORDER BY job_count DESC LIMIT 10;"

# Salary benchmark
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "USE topcv_db; SELECT * FROM fact_salary_benchmark ORDER BY avg_salary DESC;"

# Location trend
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "USE topcv_db; SELECT * FROM dim_location_trend ORDER BY job_count DESC;"
```

### Xóa checkpoint để chạy lại từ đầu

```bash
docker exec namenode hdfs dfs -rm -r /tmp/enrich_ckpt/
docker exec namenode hdfs dfs -rm -r /datalake/silver/topcv/final/
```

---

## Xử lý lỗi thường gặp

| Lỗi | Nguyên nhân | Cách xử lý |
|---|---|---|
| `No such service: spark-worker` | Container cũ chưa xóa | `docker stop spark-worker && docker rm spark-worker` |
| `Permission denied: user=spark` | HDFS chưa cấp quyền | `docker exec namenode hdfs dfs -chmod 777 /tmp` |
| `ModuleNotFoundError: requests` | Thiếu package trong container | `docker exec -u root spark-master pip install requests` |
| `LongWritable cannot be cast to IntWritable` | Schema mismatch Hive | Dùng `BIGINT` thay `INT` trong HQL |
| Job bị kill sau 2h | Network timeout | Đã fix trong `transform_enrich.py` với `spark.network.timeout=800s` |
| `== []` SparkRuntimeException | So sánh array sai kiểu | Dùng `size(col(...)) == 0` thay vì `== []` |

---

## Kết quả sau khi chạy xong

- **~1700 IT jobs** đã được enrich và lưu ở Silver layer
- **~2900 unique skills** được trích xuất
- **Top skills**: React, Python, Java, SQL, JavaScript
- **Salary benchmark**: Intern ~5tr → Lead ~36tr (triệu VND)
- **Location**: Hà Nội 62%, HCM 22%, Đà Nẵng 2%

---

## Roadmap

- [x] Bronze layer — Crawl & lưu JSON
- [x] Kafka — Message bus
- [x] Silver layer — Dedup, Clean, LLM Enrich
- [x] Hive Silver Table
- [x] Gold layer — Skills, Salary, Location aggregation
- [x] Hive Gold Tables
- [ ] FastAPI backend
- [ ] UI Dashboard
- [ ] Airflow scheduling