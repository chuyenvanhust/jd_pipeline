# s6.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, StringType
import re

# ── Unicode-aware word boundary ─────────────────────────────────────────────
# \b thông thường không nhận diện ký tự có dấu tiếng Việt (ỹ, ế, ộ, ...)
# Dùng lookaround bao phủ:
#   \u00C0-\u024F : Latin Extended (à, é, ü, ...)
#   \u1E00-\u1EFF : Latin Extended Additional (ắ, ề, ộ, ữ, ... — toàn bộ tiếng Việt)
_B  = r'(?<![a-zA-Z0-9\u00C0-\u024F\u1E00-\u1EFF])'
_BE = r'(?![a-zA-Z0-9\u00C0-\u024F\u1E00-\u1EFF])'

# Regex strip HTML — pre-compile 1 lần dùng mãi
_HTML_STRIP = re.compile(r'<[^>]+>')


def process(df: DataFrame, domain_map: dict) -> DataFrame:
    """
    Phát hiện domain kỹ thuật từ tất cả các trường dữ liệu có sẵn.

    Kỹ thuật áp dụng:
      - Longest Match First : từ khoá dài nhất được kiểm tra trước
        → "Data Engineer" không bị bắt nhầm thành "Data"
      - HTML Strip          : loại thẻ <...> trước khi quét
        → tránh bắt nhầm thuộc tính như class="python-sdk"
      - Pre-compile Regex   : compile 1 lần ngoài UDF, cache lại trong worker
        → tránh re-compile N_keywords × N_rows lần
      - Lazy Init Cache     : dùng hasattr để giữ compiled patterns
        trên mỗi Spark executor process xuyên suốt partition

    Tham số
    -------
    df         : DataFrame đầu vào, cần có các cột:
                   title, tags, tags_requirement, tags_specialization,
                   mo_ta_cong_viec_html, yeu_cau_ung_vien_html
    domain_map : dict[str, list[str]]
                   key   = tên domain chuẩn (vd "Backend", "AI/ML")
                   value = danh sách từ khoá (vd ["java", "spring boot", ...])

    Trả về
    ------
    DataFrame có thêm cột "domain" kiểu ArrayType(StringType()).
    Nếu không khớp từ khoá nào → ["Other"].
    """

    # ── Bước 1: Flatten domain_map → list[(pattern_str, domain, len)] ───────
    # Lưu pattern dạng STRING (chưa compile) để truyền an toàn vào closure.
    # Spark serialize closure → object re.Pattern không đảm bảo serialize đúng
    # trên mọi phiên bản → dùng string, compile lazy bên trong UDF.
    keyword_entries: list[tuple[str, str, int]] = []
    for domain, keywords in domain_map.items():
        for kw in keywords:
            kw_lower = kw.strip().lower()
            if not kw_lower:
                continue
            pattern_str = _B + re.escape(kw_lower) + _BE
            keyword_entries.append((pattern_str, domain, len(kw_lower)))

    # Longest Match First: sắp xếp theo độ dài keyword gốc giảm dần
    keyword_entries.sort(key=lambda x: x[2], reverse=True)

    # Bỏ trường len (chỉ dùng để sort), giữ lại (pattern_str, domain)
    pattern_domain_pairs: list[tuple[str, str]] = [
        (ps, dom) for ps, dom, _ in keyword_entries
    ]

    # ── Bước 2: Định nghĩa UDF ───────────────────────────────────────────────
    def extract_domain(
        title:     str,
        tags:      list,
        tags_req:  list,
        tags_spec: list,
        desc_html: str,
        req_html:  str,
    ) -> list:

        # Lazy init: compile regex một lần duy nhất trên mỗi executor process.
        # hasattr hoạt động như module-level cache — giữ nguyên xuyên suốt partition,
        # tránh re-compile O(N_keywords) lần cho mỗi trong O(N_rows) rows.
        if not hasattr(extract_domain, "_compiled"):
            extract_domain._compiled = [
                (re.compile(ps, re.IGNORECASE), dom)
                for ps, dom in pattern_domain_pairs
            ]
        compiled: list[tuple[re.Pattern, str]] = extract_domain._compiled

        # ── Gom nguồn dữ liệu ───────────────────────────────────────────────
        parts: list[str] = []

        # Văn bản thuần: title và các mảng tags
        if title:
            parts.append(title)
        if tags:
            parts.extend(t for t in tags if t)
        if tags_req:
            parts.extend(t for t in tags_req if t)
        if tags_spec:
            parts.extend(t for t in tags_spec if t)

        # HTML: strip thẻ trước khi đưa vào pool văn bản
        # Tránh bắt nhầm: <li class="java-developer"> → không nên match "java"
        if desc_html:
            parts.append(_HTML_STRIP.sub(" ", desc_html))
        if req_html:
            parts.append(_HTML_STRIP.sub(" ", req_html))

        if not parts:
            return ["Other"]

        # Nối tất cả nguồn bằng dấu phân cách trung lập (" | ")
        # để tránh 2 từ từ 2 nguồn khác nhau vô tình ghép thành từ mới
        full_text = " | ".join(parts).lower()

        # ── Quét từ khoá ────────────────────────────────────────────────────
        found: set[str] = set()
        for pattern, domain_name in compiled:
            if pattern.search(full_text):
                found.add(domain_name)

        # sorted() để output ổn định (dễ test, dễ debug, idempotent)
        return sorted(found) if found else ["Other"]

    domain_udf = udf(extract_domain, ArrayType(StringType()))

    # ── Bước 3: Apply UDF lên DataFrame ─────────────────────────────────────
    return df.withColumn(
        "domain",
        domain_udf(
            col("title"),
            col("tags"),
            col("tags_requirement"),
            col("tags_specialization"),
            col("mo_ta_cong_viec_html"),
            col("yeu_cau_ung_vien_html"),
        ),
    )