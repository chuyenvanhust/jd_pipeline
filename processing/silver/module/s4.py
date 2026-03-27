# s4.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import FloatType
import re

def process(df: DataFrame) -> DataFrame:
    

    # Boundary để bắt đúng đơn vị, không bắt nhầm giữa chừng từ
    _UNIT_END = r'(?![a-zA-Z])'

    # Regex khoảng năm: "2 - 3 năm", "1.5 - 2 years", "từ 2 đến 5 năm"
    YEAR_RANGE_RE = re.compile(
        r'(\d+(?:\.\d+)?)\s*[-–đếnto]+\s*(\d+(?:\.\d+)?)\s*'
        r'(?:năm|years?|yr)' + _UNIT_END,
        re.IGNORECASE
    )
    # Regex số năm đơn: "ít nhất 2 năm", "2+ years", "1.5yr"
    # Lưu ý: 'y' một mình chỉ được bắt khi KHÔNG theo sau bởi chữ cái
    YEAR_SINGLE_RE = re.compile(
        r'(\d+(?:\.\d+)?)\s*\+?\s*(?:năm|years?|yr|y)' + _UNIT_END,
        re.IGNORECASE
    )

    # Regex khoảng tháng: "6 - 12 tháng", "6-18 months"
    MONTH_RANGE_RE = re.compile(
        r'(\d+(?:\.\d+)?)\s*[-–đếnto]+\s*(\d+(?:\.\d+)?)\s*'
        r'(?:tháng|months?)' + _UNIT_END,
        re.IGNORECASE
    )
    # Regex tháng đơn: "6 tháng", "18 months"
    MONTH_SINGLE_RE = re.compile(
        r'(\d+(?:\.\d+)?)\s*(?:tháng|months?)' + _UNIT_END,
        re.IGNORECASE
    )

    # Backup: số thuần (không có đơn vị)
    BACKUP_RE = re.compile(r'(\d+(?:\.\d+)?)')

    def extract_exp(raw: str) -> float:
        if not raw:
            return None

        text = raw.lower().strip()
        text = text.replace(',', '.')  # "1,5" → "1.5"

        # ── 1. Không yêu cầu kinh nghiệm ───────────────────────────────────
        zero_keywords = [
            "không yêu cầu", "no experience", "fresher",
            "chưa có kinh nghiệm", "sinh viên", "intern"
        ]
        if any(kw in text for kw in zero_keywords):
            return 0.0

        # ── 2. Dưới 1 năm / dưới N tháng ───────────────────────────────────
        if "dưới 1 năm" in text or "dưới 1 year" in text:
            return 0.5
        if "dưới" in text and ("tháng" in text or "month" in text):
            return 0.25

        # ── 3. Khoảng tháng ─────────────────────────────────────────────────
        m = MONTH_RANGE_RE.search(text)
        if m:
            avg_months = (float(m.group(1)) + float(m.group(2))) / 2
            return round(avg_months / 12, 2)

        # ── 4. Tháng đơn ────────────────────────────────────────────────────
        m = MONTH_SINGLE_RE.search(text)
        if m:
            return round(float(m.group(1)) / 12, 2)

        # ── 5. Khoảng năm ───────────────────────────────────────────────────
        m = YEAR_RANGE_RE.search(text)
        if m:
            return (float(m.group(1)) + float(m.group(2))) / 2

        # ── 6. Năm đơn ──────────────────────────────────────────────────────
        m = YEAR_SINGLE_RE.search(text)
        if m:
            return float(m.group(1))

        # ── 7. Backup: số thuần không có đơn vị (vd TopCV chỉ ghi "2") ─────
        m = BACKUP_RE.search(text)
        if m:
            val = float(m.group(1))
            # Giới hạn < 20 để tránh bắt nhầm job_id, năm sinh, số điện thoại
            return val if val < 20 else None

        return None

    exp_udf = udf(extract_exp, FloatType())
    return df.withColumn("years_of_experience", exp_udf(col("experience")))
