-- ═══════════════════════════════════════════════════════════
-- gold_tables.hql
-- Tạo Hive External Tables cho Gold layer
-- Chạy SAU KHI các file agg_*.py đã ghi data lên HDFS
-- Chạy: docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /gold_tables.hql
-- ═══════════════════════════════════════════════════════════

USE topcv_db;

-- ─────────────────────────────────────────
-- 1. dim_skills_demand
--    Nguồn: agg_skills.py
--    Mỗi row = 1 kỹ năng + thống kê nhu cầu thị trường
-- ─────────────────────────────────────────
DROP TABLE IF EXISTS dim_skills_demand;

CREATE EXTERNAL TABLE dim_skills_demand (
    skill_name   STRING,   -- Tên kỹ năng đã normalize, VD "Python", "React"
    job_count    BIGINT,      -- Số JD yêu cầu kỹ năng này
    percentage   DOUBLE,   -- % trên tổng số IT JD
    avg_salary   DOUBLE,   -- Lương trung bình của JD có kỹ năng này (triệu VND)
    updated_date DATE      -- Ngày chạy pipeline
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/datalake/gold/reports/dim_skills_demand/';

-- ─────────────────────────────────────────
-- 2. fact_salary_benchmark
--    Nguồn: agg_salary.py
--    Mỗi row = 1 job_level + thống kê lương
-- ─────────────────────────────────────────
DROP TABLE IF EXISTS fact_salary_benchmark;

CREATE EXTERNAL TABLE fact_salary_benchmark (
    job_level            STRING,   -- Junior/Middle/Senior/Lead/Manager/Intern
    job_count            BIGINT,      -- Số mẫu
    avg_salary           DOUBLE,   -- Lương trung bình (triệu VND)
    min_salary           DOUBLE,   -- Lương thấp nhất
    max_salary           DOUBLE,   -- Lương cao nhất
    years_of_experience  DOUBLE,   -- Số năm KN trung bình
    updated_date         DATE      -- Ngày chạy pipeline
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/datalake/gold/reports/fact_salary_benchmark/';

-- ─────────────────────────────────────────
-- 3. dim_location_trend
--    Nguồn: agg_location.py
--    Mỗi row = 1 tỉnh/thành + thống kê thị trường
-- ─────────────────────────────────────────
DROP TABLE IF EXISTS dim_location_trend;

CREATE EXTERNAL TABLE dim_location_trend (
    city         STRING,   -- Hà Nội / Hồ Chí Minh / Đà Nẵng / Toàn quốc / Khác
    job_count    BIGINT,      -- Số JD tại địa điểm này
    percentage   DOUBLE,   -- % trên tổng số IT JD
    avg_salary   DOUBLE,   -- Lương trung bình (triệu VND)
    top_skills   STRING,   -- Top 5 kỹ năng phổ biến nhất, phân cách bởi ", "
    updated_date DATE      -- Ngày chạy pipeline
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/datalake/gold/reports/dim_location_trend/';

-- ─────────────────────────────────────────
-- Kiểm tra
-- ─────────────────────────────────────────
SHOW TABLES;
