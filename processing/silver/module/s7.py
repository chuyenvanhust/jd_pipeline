# s7.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, StringType
import re

# ── Unicode-aware word boundary ─────────────────────────────────────────────
# Hỗ trợ ký tự đặc biệt trong tên skill: C#, C++, .NET
# Hỗ trợ tiếng Việt: không bị lỗi với ắ, ề, ộ, ữ...
_B  = r'(?<![a-zA-Z0-9\u00C0-\u024F\u1E00-\u1EFF])'
_BE = r'(?![a-zA-Z0-9\u00C0-\u024F\u1E00-\u1EFF])'

# Pre-compile HTML stripper một lần ở module level
_HTML_STRIP = re.compile(r'<[^>]+>')


def process(df: DataFrame, skill_dictionary: dict) -> DataFrame:
    """
    Trích xuất kỹ năng từ nhiều cột nguồn, CHỈ quét skill thuộc domain
    đã được phát hiện ở bước s6 (cột `domain`).

    Lý do giới hạn theo domain:
      - Tránh false-positive: từ "R" (ngôn ngữ lập trình) có thể bắt nhầm
        trong văn bản tiếng Việt nếu không có ngữ cảnh domain AI Scientist.
      - Tăng tốc: số skill cần quét giảm đáng kể (chỉ quét pool nhỏ).
      - Kết quả có ngữ nghĩa rõ ràng hơn theo từng domain.

    Luồng xử lý:
      1. Với mỗi row, đọc list domain từ cột `domain` (output của s6).
      2. Gom tất cả skill thuộc các domain đó → pool skill cần quét.
      3. Build / lấy từ cache regex cho pool đó.
      4. Quét toàn bộ văn bản (title, tags, HTML) bằng regex.
      5. Trả về danh sách skill tìm được (đã dedupe, chuẩn hoá, sort).

    Tham số
    -------
    df               : DataFrame đầu vào — phải đã có cột `domain`
                       (chạy s6 trước).
                       Cần có thêm: title, tags, tags_requirement,
                       tags_specialization, mo_ta_cong_viec_html,
                       yeu_cau_ung_vien_html
    skill_dictionary : dict[str, list[str]]
                       key   = tên domain (phải khớp với giá trị trong cột domain)
                       value = danh sách skill thuộc domain đó

    Trả về
    ------
    DataFrame có thêm cột "skills" kiểu ArrayType(StringType()).
    Nếu không tìm được skill nào → [].
    """

    # ── Bước 1: Chuẩn hoá skill_dictionary ──────────────────────────────────
    # Với mỗi domain → sort skill dài → ngắn (C++ trước C, .NET trước NET)
    # để Longest Match First hoạt động đúng khi build alternation regex.
    normalized = {}  # dict[str, list[str]]
    for domain, skills in skill_dictionary.items():
        # Dedupe trong cùng domain, giữ casing gốc
        unique = list({s.strip() for s in skills if s.strip()})
        unique.sort(key=len, reverse=True)
        normalized[domain] = unique

    # ── Bước 2: Tạo skill_map toàn cục (lowercase → casing chuẩn) ───────────
    # Dùng để map "python" → "Python", "c#" → "C#" sau khi regex tìm được match.
    # Ưu tiên skill dài hơn nếu 2 skill cùng lowercase (hiếm, nhưng an toàn).
    skill_map = {}  # dict[str, str] — lowercase → casing chuẩn
    for skills in normalized.values():
        for s in skills:
            key = s.lower()
            # Giữ skill dài hơn nếu trùng key (vd "NodeJS" vs "Nodejs")
            if key not in skill_map or len(s) > len(skill_map[key]):
                skill_map[key] = s

    # ── Bước 3: Pre-build pattern string cho từng domain ────────────────────
    # Lưu dạng STRING để truyền an toàn qua Spark closure serialize.
    # re.Pattern object không đảm bảo picklable trên mọi môi trường.
    domain_pattern_str = {}  # dict[str, str] — domain → pattern string
    for domain, skills in normalized.items():
        if not skills:
            continue
        escaped = [re.escape(s) for s in skills]
        # Alternation: skill dài nhất ở đầu (đã sort ở Bước 1)
        domain_pattern_str[domain] = (
            _B + r'(' + '|'.join(escaped) + r')' + _BE
        )

    # ── Bước 4: Định nghĩa UDF ───────────────────────────────────────────────
    def extract_skills(
        domains:   list,   # từ cột `domain` (output s6), vd ["Backend Developer", "DevOps Engineer"]
        title:     str,
        tags:      list,
        tags_req:  list,
        tags_spec: list,
        desc_html: str,
        req_html:  str,
    ) -> list:

        # Lazy init cache: dict[domain_str → compiled re.Pattern]
        # Compile khi gặp domain lần đầu, giữ nguyên suốt partition.
        if not hasattr(extract_skills, "_cache"):
            extract_skills._cache = {}  # dict[domain_str, compiled re.Pattern]

        cache = extract_skills._cache  # dict[domain_str, compiled re.Pattern]

        # Không có domain → không biết quét skill gì
        if not domains:
            return []

        # ── Gom pool skill cần quét dựa theo domain ─────────────────────────
        # Mỗi domain có 1 regex pattern riêng → compile lazy khi cần
        patterns_to_use = []  # list of compiled re.Pattern
        for domain in domains:
            if domain == "Other" or domain not in domain_pattern_str:
                continue
            if domain not in cache:
                cache[domain] = re.compile(
                    domain_pattern_str[domain], re.IGNORECASE
                )
            patterns_to_use.append(cache[domain])

        if not patterns_to_use:
            return []

        # ── Gom và làm sạch văn bản nguồn ───────────────────────────────────
        parts = []  # list[str]
        if title:
            parts.append(title)
        if tags:
            parts.extend(t for t in tags if t)
        if tags_req:
            parts.extend(t for t in tags_req if t)
        if tags_spec:
            parts.extend(t for t in tags_spec if t)
        if desc_html:
            parts.append(_HTML_STRIP.sub(" ", desc_html))
        if req_html:
            parts.append(_HTML_STRIP.sub(" ", req_html))

        if not parts:
            return []

        # Nối bằng " | " tránh 2 token từ 2 nguồn ghép thành từ mới
        full_text = " | ".join(parts)

        # ── Quét từng pattern theo domain ───────────────────────────────────
        found = set()  # set[str]
        for pattern in patterns_to_use:
            for m in pattern.findall(full_text):
                key = m.lower()
                if key in skill_map:
                    found.add(skill_map[key])

        # sort để output ổn định (idempotent, dễ test)
        return sorted(found)

    skills_udf = udf(extract_skills, ArrayType(StringType()))

    # ── Bước 5: Apply UDF lên DataFrame ─────────────────────────────────────
    return df.withColumn(
        "skills",
        skills_udf(
            col("domain"),                  # ← từ s6, dùng để chọn pool skill
            col("title"),
            col("tags"),
            col("tags_requirement"),
            col("tags_specialization"),
            col("mo_ta_cong_viec_html"),
            col("yeu_cau_ung_vien_html"),
        ),
    )