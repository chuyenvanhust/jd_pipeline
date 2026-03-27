#config.py
from pyspark.sql.types import (
    StructType, StringType, ArrayType,
    
)

# ─────────────────────────────────────────
#  PATH
# ─────────────────────────────────────────
INPUT_PATH = "hdfs://namenode:9000/datalake/silver/topcv/stage_unique/"
OUTPUT_PATH = "hdfs://namenode:9000/datalake/silver/topcv/silver_process/"

# ─────────────────────────────────────────
#  SCHEMA
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
# 0. IT FILTER - Có thể dùng DOMAIN ,nếu có trogn DOMAIN thì đạt ,không thì không đạt 
# ─────────────────────────────────────────
# 
IT_KEYWORDS = [
    # Tiếng Việt
    "lập trình viên", "kỹ sư phần mềm", "phát triển phần mềm", "kiểm thử", "hệ thống mạng",
    "quản trị mạng", "an toàn thông tin", "trí tuệ nhân tạo", "dữ liệu", "phân tích nghiệp vụ",
    # Tiếng Anh & Phổ biến
    "developer", "engineer", "software", "backend", "frontend", "fullstack", "devops",
    "tester", "qa", "qc", "data analyst", "data scientist", "ai", "machine learning",
    "product owner", "product manager", "scrum master", "system admin", "cloud",
    "ui/ux", "designer", "solution architect", "mobile app", "android", "ios", "embedded"
]

NON_IT_KEYWORDS = [
    "kế toán", "bảo vệ", "tài xế", "công nhân", "lễ tân", "nhân viên tư vấn", 
    "telesale", "bán hàng", "marketing online", "nhân sự", "hành chính", "quản lý kho","sale",
    "sales" ,"accounting", "security guard", "driver", "worker", "receptionist", "consultant",
    "telesales", "salesperson", "marketer", "hr", "admin", "warehouse manager", "logistics", "supply chain", "customer service", "call center",
    
]

# ─────────────────────────────────────────
# 1. COMPANY NAME
# ─────────────────────────────────────────
COMPANY_MAP = {
   
}


# 2. SALARY UNITS (Xử lý đơn vị tiền tệ)
# ─────────────────────────────────────────
SALARY_UNIT_MAP = {
    "VND": ["triệu", "tr", "vnd", "vnđ", "m"],
    "USD": ["usd", "$", "đô", "đô la"],
    "JPY": [
        "jpy", "yen", "yên", "¥", 
        "man", "lá",          # 1 man = 10.000 Yên (Rất phổ biến trong JD đi Nhật)
        "sen", "sên"          # 1 sen = 1.000 Yên
    ],
    "EUR": ["eur", "euro", "€", "ơ-rô"]
    
}
# 3. LOCATION MAPPING (Ánh xạ 63 tỉnh thành cũ về 34 đơn vị mới)
# ────────────────────────────────────────────────────────
LOCATION_MAP = {
    # --- 11 Đơn vị không thực hiện sắp xếp ---
    "Hà Nội": ["hà nội", "ha noi", "hn", "hanoi"],
    "Thanh Hóa": ["thanh hóa", "thanh hoa"],
    "Lạng Sơn": ["lạng sơn", "lang son"],
    "Thừa Thiên Huế": ["thừa thiên huế", "thue", "tp huế", "tp hue", "thua thien hue"],
    "Nghệ An": ["nghệ an", "nghe an"],
    "Hà Tĩnh": ["hà tĩnh", "ha tinh"],
    "Lai Châu": ["lai châu", "lai chau"],
    "Điện Biên": ["điện biên", "dien bien"],
    "Cao Bằng": ["cao bằng", "cao bang"],
    "Quảng Ninh": ["quảng ninh", "quang ninh"],
    "Sơn La": ["sơn la", "son la"],

    # --- 23 Đơn vị hành chính cấp tỉnh mới (Sau sáp nhập) ---
    "Tuyên Quang": ["tuyên quang", "tuyen quang", "hà giang", "ha giang"],
    "Lào Cai": ["lào cai", "lao cai", "yên bái", "yen bai"],
    "Thái Nguyên": ["thái nguyên", "thai nguyen", "bắc kạn", "bac kan", "bk"],
    "Phú Thọ": ["phú thọ", "phu tho", "vĩnh phúc", "vinh phuc", "hòa bình", "hoa binh", "vp"],
    "Bắc Ninh": ["bắc ninh", "bac ninh", "bắc giang", "bac giang", "bn", "bg"],
    "Hưng Yên": ["hưng yên", "hung yen", "thái bình", "thai binh"],
    "Hải Phòng": ["hải phòng", "hai phong", "hp", "hải dương", "hai duong", "hd"],
    "Ninh Bình": ["ninh bình", "ninh binh", "nam định", "nam dinh", "hà nam", "ha nam"],
    "Quảng Trị": ["quảng trị", "quang tri", "quảng bình", "quang binh", "qb"],
    "Đà Nẵng": ["đà nẵng", "da nang", "dn", "quảng nam", "quang nam"],
    "Quảng Ngãi": ["quảng ngãi", "quang ngai", "kon tum", "kontum"],
    "Gia Lai": ["gia lai", "gialai", "bình định", "binh dinh", "quy nhơn"],
    "Khánh Hòa": ["khánh hòa", "khanh hoa", "nha trang", "ninh thuận", "ninh thuan"],
    "Lâm Đồng": ["lâm đồng", "lam dong", "đà lạt", "bình thuận", "binh thuan", "đắk nông", "dak nong"],
    "Đắk Lắk": ["đắk lắk", "dak lak", "đắc lắc", "phú yên", "phu yen"],
    "TP.HCM": [
        "hồ chí minh", "ho chi minh", "hcm", "tphcm", "sài gòn", "saigon", 
        "bình dương", "binh duong", "bd", "bà rịa vũng tàu", "vũng tàu", "brvt"
    ],
    "Đồng Nai": ["đồng nai", "dong nai", "đn", "bình phước", "binh phuoc"],
    "Tây Ninh": ["tây ninh", "tay ninh", "long an", "la"],
    "Cần Thơ": ["cần thơ", "can tho", "ct", "sóc trăng", "soc trang", "hậu giang", "hau giang"],
    "Vĩnh Long": ["vĩnh long", "vinh long", "bến tre", "ben tre", "trà vinh", "tra vinh"],
    "Đồng Tháp": ["đồng tháp", "dong thap", "tiền giang", "tien giang"],
    "Cà Mau": ["cà mau", "ca mau", "bạc liêu", "bac lieu"],
    "An Giang": ["an giang", "an giang", "kiên giang", "kien giang", "rạch giá"],

    # --- Hình thức làm việc đặc biệt ---
    "Remote": ["tại nhà", "từ xa", "remote", "work from home", "wfh", "toàn quốc"]
}
# ─────────────────────────────────────────
# 4. EXPERIENCE UNITS
# ─────────────────────────────────────────
EXP_MAP = {
    
}
#regex số năm ,không cần map 

# ─────────────────────────────────────────
# 5. LEVEL MAPPING (Chuẩn hóa cấp bậc)
# ─────────────────────────────────────────
LEVEL_MAP = {
    "Intern": ["intern", "thực tập", "học việc","sinh viên"],
    "Fresher": ["fresher", "mới tốt nghiệp", "entry level","ra trường"],
    "Junior": ["junior", "dưới 2 năm", "1 năm"],
    "Middle": ["middle", "mid-level", "2 năm", "3 năm"],
    "Senior": ["senior", "cao cấp","4 năm", "5 năm", "dày dạn"],
    "Lead": ["leader", "trưởng nhóm", "lead", "techlead"],
    "Manager": ["manager", "quản lý", "trưởng phòng", "director", "cto", "cio"]
}

# 6. DOMAIN MAPPING (Bổ sung Quản lý, QA, Design và Hệ thống)
# ─────────────────────────────────────────
DOMAIN_MAP = {
    # --- Nhóm Dữ liệu & AI ---
    "AI Engineer": ["ai engineer", "ml engineer", "machine learning engineer", "kỹ sư ai", "kỹ sư trí tuệ nhân tạo", "lập trình ai"],
    "Data Engineer": ["data engineer", "de", "kỹ sư dữ liệu", "big data engineer", "data pipeline", "etl engineer"],
    "AI Researcher": ["ai researcher", "research engineer", "nghiên cứu ai", "nghiên cứu trí tuệ nhân tạo", "algorithm researcher"],
    "AI Scientist": ["ai scientist", "data scientist", "ds", "machine learning scientist", "nhà khoa học dữ liệu"],
    "Data Analyst": ["data analyst", "da", "phân tích dữ liệu", "bi analyst", "business intelligence", "data visualization"],
    "Business Analyst": ["business analyst", "ba", "phân tích nghiệp vụ", "it ba", "requirement engineer", "ba software"],

    # --- Nhóm Phát triển Phần mềm ---
    "Backend Developer": ["backend", "back-end", "lập trình hệ thống", "server-side", "java developer", "python developer", "php developer", ".net developer", "nodejs developer"],
    "Frontend Developer": ["frontend", "front-end", "lập trình giao diện", "client-side", "web developer", "react developer", "angular developer", "vue developer"],
    "Fullstack Developer": ["fullstack", "full-stack", "lập trình toàn diện", "end-to-end developer"],
    "Mobile Developer": ["mobile developer", "lập trình di động", "ios developer", "android developer", "flutter developer", "react native developer", "app developer"],
    "Game Developer": ["game developer", "unity", "unreal engine", "lập trình game", "cocos", "game engine"],

    # --- Nhóm Kiểm thử & Chất lượng (QA/QC/Tester) ---
    "QA/QC/Tester": [
        "qa", "qc", "tester", "kiểm thử", "quality assurance", "quality control", 
        "automation test", "manual test", "pqc", "sqc", "test engineer", "kiểm soát chất lượng"
    ],

    # --- Nhóm Quản lý & Quy trình (Management/Product) ---
    "Project Management": [
        "project manager", "pm", "quản lý dự án", "pmp", "project lead", 
        "delivery manager", "it manager", "trưởng dự án"
    ],
    "Product Management": [
        "product manager", "product owner", "po", "quản lý sản phẩm", "giám đốc sản phẩm"
    ],
    "Scrum Master": ["scrum master", "agile coach" ,"scrum"],

    # --- Nhóm Hạ tầng, Hệ thống & Bảo mật ---
    "DevOps Engineer": ["devops", "site reliability engineer", "sre", "automation engineer", "triển khai hệ thống", "ci/cd engineer"],
    "Cloud Engineer": ["cloud engineer", "aws engineer", "azure engineer", "gcp engineer", "hạ tầng đám mây", "cloud architect"],
    "Cyber Security": ["cyber security", "security engineer", "an toàn thông tin", "bảo mật hệ thống", "pentest", "soc", "attt", "information security"],
    "System Admin": [
        "system admin", "quản trị hệ thống", "sysadmin", "it helpdesk", "it support", 
        "it technical", "network engineer", "kỹ sư mạng", "kỹ sư hệ thống"
    ],

    # --- Nhóm Thiết kế (UI/UX/Design) ---
    "UI/UX Designer": [
        "ui/ux", "ui-ux", "ux designer", "ui designer", "thiết kế giao diện", 
        "thiết kế trải nghiệm", "product designer", "web designer"
    ],

    # --- Nhóm Chuyên biệt & Phần cứng ---
    "IoT/Embedded": [
        "iot engineer", "internet of things", "lập trình nhúng", "embedded engineer", 
        "firmware developer", "vi điều khiển", "hardware engineer", "kỹ sư phần cứng"
    ],
    "Bridge Engineer": ["bridge engineer", "brse", "kỹ sư cầu nối", "tiếng nhật"],
    "ERP/SAP": ["erp", "sap", "odoo", "oracle apps", "dynamics 365", "tư vấn erp"]
}


# 7. SKILL DICTIONARY (Ánh xạ theo từng Domain chuyên biệt)
# ────────────────────────────────────────────────────────
SKILL_DICTIONARY = {
    # --- Nhóm Dữ liệu & AI ---
    "AI Engineer": ["Python", "PyTorch", "TensorFlow", "Keras", "Scikit-learn", "OpenCV", "LLM", "NLP", "Deep Learning", "Generative AI", "HuggingFace", "Neural Networks"],
    "Data Engineer": ["SQL", "Python", "Spark", "Kafka", "Hadoop", "Airflow", "ETL", "Data Warehouse", "NoSQL", "dbt", "BigQuery", "Data Pipeline", "Hive"],
    "AI Researcher": ["Algorithm", "Mathematics", "Statistics", "Paper Implementation", "Research", "Python", "C++", "Latex", "Deep Learning Theory"],
    "AI Scientist": ["Statistics", "R", "Python", "Machine Learning", "Mathematics", "Deep Learning", "Data Modeling", "Hypothesis Testing", "Probability"],
    "Data Analyst": ["SQL", "Excel", "Tableau", "Power BI", "Data Visualization", "Google Analytics", "Statistics", "Python", "Cleaning Data", "Looker"],
    "Business Analyst": ["Requirement Gathering", "User Story", "UML", "BPMN", "Jira", "Confluence", "SQL", "Wireframe", "Documentation", "Product Backlog", "Gap Analysis"],

    # --- Nhóm Phát triển Phần mềm ---
    "Backend Developer": ["Java", "Spring Boot", "Nodejs", "PHP", "Laravel", "Python", "Django", "Golang", "C#", ".NET", "Microservices", "REST API", "gRPC", "MySQL", "PostgreSQL", "Redis"],
    "Frontend Developer": ["HTML", "CSS", "JavaScript", "TypeScript", "React", "Angular", "Vue", "Next.js", "Tailwind CSS", "Sass", "Webpack", "Redux", "Bootstrap"],
    "Fullstack Developer": ["Java", "Python", "Nodejs", "React", "Vue", "SQL", "NoSQL", "DevOps basics", "Architecture", "System Design"],
    "Mobile Developer": ["Swift", "Kotlin", "Objective-C", "Dart", "Flutter", "React Native", "iOS", "Android", "Firebase", "Xcode", "Android Studio"],
    "Game Developer": ["Unity", "C#", "C++", "Unreal Engine", "Cocos2dx", "Shaders", "3D Modeling", "Blender", "Physics Engine", "Multiplayer Programming"],

    # --- Nhóm Kiểm thử & Chất lượng (QA/QC/Tester) ---
    "QA/QC/Tester": ["Selenium", "Appium", "JMeter", "Postman", "Cypress", "Cucumber", "Manual Testing", "Automation Testing", "Unit Test", "Integration Test", "Regression Testing", "Bug Tracking"],

    # --- Nhóm Quản lý & Quy trình ---
    "Project Management": ["Agile", "Scrum", "Kanban", "PMP", "Waterfall", "Risk Management", "MS Project", "Team Management", "Budgeting", "Roadmap", "Timeline"],
    "Product Management": ["Product Roadmap", "User Growth", "Market Research", "Product Strategy", "A/B Testing", "MVP", "Prioritization", "Customer Feedback", "Figma"],
    "Scrum Master": ["Scrum Guide", "Sprint Planning", "Retrospective", "Sprint Review", "Facilitation", "Agile Coaching", "Servant Leadership", "Impediment Removal"],

    # --- Nhóm Hạ tầng, Hệ thống & Bảo mật ---
    "DevOps Engineer": ["Docker", "Kubernetes", "Jenkins", "CI/CD", "Terraform", "Ansible", "Bash Script", "Monitoring", "Prometheus", "Grafana", "GitLab CI"],
    "Cloud Engineer": ["AWS", "Azure", "GCP", "CloudFormation", "Lambda", "S3", "EC2", "VPC", "Serverless", "Cloud Migration"],
    "Cyber Security": ["Pentest", "Firewall", "SOC", "OWASP", "Information Security", "Kali Linux", "Vulnerability Assessment", "ISO 27001", "Cryptography", "Identity Management"],
    "System Admin": ["Linux", "Windows Server", "Networking", "Cisco", "Helpdesk", "Active Directory", "Virtualization", "VMware", "Backup & Recovery", "Hardware Troubleshooting"],

    # --- Nhóm Thiết kế (UI/UX/Design) ---
    "UI/UX Designer": ["Figma", "Adobe XD", "Sketch", "Photoshop", "Illustrator", "Wireframing", "Prototyping", "UX Research", "User Flow", "Design System", "Visual Design"],

    # --- Nhóm Chuyên biệt & Phần cứng ---
    "IoT/Embedded": ["C", "C++","ESP","ESP32", "RTOS", "Microcontroller", "Arduino", "Raspberry Pi", "Firmware", "Circuit Design", "I2C", "SPI", "UART", "FPGA"],
    "Bridge Engineer": ["N1","N2","N3", "Japanese N1", "Japanese N2", "Japanese N3", "Interpretation", "Translation", "Communication", "Technical Document Translation", "JLPT"],
    "ERP/SAP": ["SAP", "ABAP", "Odoo", "Oracle ERP", "Dynamics 365", "ERP Implementation", "Functional Consulting", "Business Process Mapping"]
}