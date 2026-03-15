-- ═══════════════════════════════════════════════════════════
-- silver_table.hql
-- Tạo Hive Managed Table trỏ vào Silver Parquet trên HDFS
-- Chạy: docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /silver_table.hql
-- ═══════════════════════════════════════════════════════════

-- Tạo database nếu chưa có
-- IF NOT EXISTS: không báo lỗi nếu database đã tồn tại rồi
-- LOCATION: chỉ định thư mục gốc trên HDFS chứa tất cả tables của database này
CREATE DATABASE IF NOT EXISTS topcv_db
LOCATION 'hdfs://namenode:9000/datalake/';

-- Chuyển sang dùng database topcv_db cho các lệnh phía dưới
-- Tương đương USE database trong SQL thông thường
USE topcv_db;

-- Xóa table cũ nếu có — tránh lỗi "table already exists" khi chạy lại
-- IF EXISTS: không báo lỗi nếu table chưa tồn tại
-- Vì là EXTERNAL table, lệnh DROP chỉ xóa metadata trong Hive Metastore
-- Data Parquet thực tế trên HDFS KHÔNG bị xóa — an toàn khi chạy lại
DROP TABLE IF EXISTS topcv_silver;

-- EXTERNAL TABLE: Hive chỉ quản lý metadata (schema, partition info)
-- Khác MANAGED TABLE: nếu DROP TABLE thì data Parquet trên HDFS vẫn còn
-- Đây là lựa chọn đúng vì data được tạo bởi Spark, không phải Hive
CREATE EXTERNAL TABLE topcv_silver (

    -- ── Các field từ Bronze (giữ nguyên từ crawler) ─────────
    -- job_id: mã định danh duy nhất mỗi job trên TopCV
    job_id                  STRING,

    -- title: tên vị trí tuyển dụng, VD "Senior Java Developer"
    title                   STRING,

    -- company: tên công ty đăng tuyển
    company                 STRING,

    -- url: đường link bài đăng gốc trên TopCV
    url                     STRING,

    -- salary: chuỗi lương thô từ crawler, VD "10 - 20 triệu"
    -- (đã được parse sang min_salary/max_salary ở Silver)
    salary                  STRING,

    -- location: chuỗi địa điểm thô, VD "Hà Nội & 3 nơi khác"
    -- (đã được parse sang locations[] ở Silver bởi LLM)
    location                STRING,

    -- experience: chuỗi kinh nghiệm thô, VD "2 - 3 năm"
    -- (đã được parse sang exp_min/exp_max ở Silver)
    experience              STRING,

    -- tags: mảng tags phân loại kỹ năng từ TopCV, VD ["Java", "Spring"]
    -- ARRAY<STRING>: kiểu dữ liệu mảng trong Hive
    tags                    ARRAY<STRING>,

    -- tags_requirement: mảng tags yêu cầu từ TopCV
    tags_requirement        ARRAY<STRING>,

    -- tags_specialization: mảng tags vị trí chuyên môn từ TopCV
    tags_specialization     ARRAY<STRING>,

    -- deadline: hạn nộp hồ sơ dạng chuỗi, VD "31/03/2026"
    deadline                STRING,

    -- ── Các field Silver: clean text (từ transform_clean.py) ─
    -- mo_ta_text: mô tả công việc đã strip HTML, decode entities
    mo_ta_text              STRING,

    -- yeu_cau_text: yêu cầu ứng viên đã strip HTML
    yeu_cau_text            STRING,

    -- quyen_loi_text: quyền lợi đã strip HTML
    quyen_loi_text          STRING,

    -- ── Các field Silver: LLM enriched (từ transform_enrich.py)
    -- technical_skills: mảng kỹ năng kỹ thuật LLM trích xuất
    -- VD ["Python", "Spark", "SQL", "Docker"]
    technical_skills        ARRAY<STRING>,

    -- job_level: cấp bậc LLM phân loại
    -- Giá trị: Junior | Middle | Senior | Lead | Manager | Intern | Unknown
    job_level               STRING,

    -- is_it_job: true nếu là vị trí IT thực sự (Dev/QA/DevOps/Data...)
    -- false nếu là Sales, Designer đồ họa, Giáo viên...
    is_it_job               BOOLEAN,

    -- min_salary: lương tối thiểu đã parse, đơn vị triệu VND
    -- -1 nếu không xác định được (lương thỏa thuận không có hint)
    min_salary              INT,

    -- max_salary: lương tối đa đã parse, đơn vị triệu VND
    -- -1 nếu không xác định được
    max_salary              INT,

    -- salary_type: loại lương — "range" | "negotiable" | "unknown"
    salary_type             STRING,

    -- exp_min: số năm kinh nghiệm tối thiểu, FLOAT vì có thể là 0.5
    -- -1.0 nếu không xác định
    exp_min                 FLOAT,

    -- exp_max: số năm kinh nghiệm tối đa LLM đọc từ JD
    -- VD "3-5 năm" → exp_max = 5.0 | -1.0 nếu không rõ
    exp_max                 FLOAT,

    -- exp_type: loại kinh nghiệm — "range" | "min" | "none" | "unknown"
    exp_type                STRING,

    -- locations: mảng tỉnh/thành LLM chuẩn hóa từ location_raw
    -- VD ["Hà Nội", "Hồ Chí Minh"] hoặc ["Toàn quốc"]
    locations               ARRAY<STRING>,

    -- max_salary_verified: lương tối đa xác nhận từ phần quyền lợi
    -- Chỉ có khi salary_type = "negotiable" và JD mention con số cụ thể
    -- -1 nếu không có
    max_salary_verified     INT,

    -- classification_source: nguồn phân loại is_it_job
    -- "rule" = phân loại bởi regex rule | "llm" = phân loại bởi Ollama
    classification_source   STRING,

    -- source: nguồn dữ liệu, luôn là "topcv" trong pipeline này
    source                  STRING

)
-- PARTITIONED BY: chia data thành các thư mục con theo ingestion_date
-- Hive sẽ map ingestion_date=2026-03-15/ trên HDFS thành partition
-- Khi query WHERE ingestion_date = '2026-03-15', Hive chỉ đọc thư mục đó
-- → tăng tốc query đáng kể khi data nhiều ngày
PARTITIONED BY (ingestion_date DATE)

-- STORED AS PARQUET: báo Hive biết file trên HDFS là định dạng Parquet
-- Hive sẽ dùng Parquet reader thay vì text reader mặc định
STORED AS PARQUET

-- LOCATION: thư mục HDFS chứa data Parquet của table này
-- Hive KHÔNG copy data — chỉ đăng ký đường dẫn này vào Metastore
-- Data thực tế vẫn nằm nguyên tại đây do Spark đã ghi trước đó
LOCATION 'hdfs://namenode:9000/datalake/silver/topcv/final/';

-- MSCK REPAIR TABLE: quét HDFS tìm các thư mục partition mới
-- Cần chạy vì Spark ghi partition trực tiếp lên HDFS mà không báo Hive
-- Nếu không chạy: Hive biết table tồn tại nhưng không thấy partition nào
-- → query trả về 0 rows dù data có đầy trên HDFS
MSCK REPAIR TABLE topcv_silver;

-- ── Kiểm tra kết quả ─────────────────────────────────────────

-- Liệt kê tất cả partition đã được nhận diện
-- Kết quả mong đợi: ingestion_date=2026-03-15
SHOW PARTITIONS topcv_silver;

-- Đếm tổng số IT jobs trong Silver
-- Mong đợi: số dương, khoảng vài trăm đến vài nghìn
SELECT COUNT(*) AS total_jobs FROM topcv_silver;

-- Phân bố theo job_level — kiểm tra chất lượng LLM enrichment
-- Nếu Unknown > 30% thì prompt cần cải thiện
SELECT job_level, COUNT(*) AS cnt FROM topcv_silver GROUP BY job_level ORDER BY cnt DESC;
