# s2.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, FloatType, StringType
import re

def process(df: DataFrame, salary_unit_map: dict) -> DataFrame:

    schema = StructType([
        StructField("min_salary", FloatType()),
        StructField("max_salary", FloatType()),
        StructField("currency",   StringType()),
    ])

    # Tỷ giá quy đổi về đơn vị Triệu VND
    USD_TO_M_VND = 0.025        # 1,000 USD  → ~25 Triệu VND
    EUR_TO_M_VND = 0.027        # 1,000 EUR  → ~27 Triệu VND
    MAN_TO_M_VND = 1.65         # 1 Man      → ~1.65 Triệu VND
    YEN_TO_M_VND = 0.000165     # 1 Yên      → 0.000165 Triệu VND

    # Ngưỡng hợp lý tối đa (Triệu VND) — lọc outlier do parse sai
    # Lương IT VN cao nhất thực tế ~200 triệu/tháng
    MAX_SALARY_M_VND = 500.0

    def parse_salary(raw):
        if not raw:
            return (None, None, None)

        original = raw.lower().strip()

        # ── 1. Xác định Currency ────────────────────────────────────────────
        currency = "VND"
        for cur, keywords in salary_unit_map.items():
            if any(kw in original for kw in keywords):
                currency = cur
                break

        # ── 2. Chuẩn hoá dấu phân cách ─────────────────────────────────────
        # Xoá dấu phẩy phân cách nghìn: 1,000,000 → 1000000
        text = original.replace(',', '')

        # Xử lý dấu chấm THÔNG MINH:
        # Lỗi cũ: if count('.') >= 2 → xoá hết → "13.8 - 34.5" mất dấu thập phân
        #
        # Logic đúng: dấu chấm phân cách nghìn kiểu VN/EU có dạng X.XXX (đúng 3 chữ số sau)
        # Ví dụ: "10.000.000" → mỗi nhóm sau chấm đều là 3 chữ số → xoá chấm
        # Còn "13.8 - 34.5" → sau chấm chỉ có 1 chữ số → là thập phân, giữ lại
        def normalize_dots(s):
            # Tìm tất cả dấu chấm, kiểm tra từng cái
            # Nếu TẤT CẢ dấu chấm đều có đúng 3 chữ số theo sau → dấu phân cách nghìn
            dot_positions = [m.start() for m in re.finditer(r'\.', s)]
            if not dot_positions:
                return s
            all_thousand_sep = all(
                re.match(r'\.\d{3}(?!\d)', s[pos:]) for pos in dot_positions
            )
            if all_thousand_sep:
                return s.replace('.', '')
            return s  # giữ nguyên nếu có dấu thập phân

        text = normalize_dots(text)

        # ── 3. Tách số ──────────────────────────────────────────────────────
        numbers = re.findall(r'\d+(?:\.\d+)?', text)
        numbers = [float(n) for n in numbers]

        if not numbers:
            return (None, None, currency)

        # ── 4. Quy đổi về Triệu VND ─────────────────────────────────────────
        def convert_to_million(val, cur, orig_text):
            if val is None:
                return None
            try:
                val = float(val)
            except (TypeError, ValueError):
                return None

            if cur == "VND":
                # >= 100,000 → đơn vị VND thô (vd 15000000) → chia về triệu
                if val >= 100_000:
                    result = val / 1_000_000
                else:
                    # Số nhỏ → đã là triệu (vd "15 triệu", "13.8 triệu")
                    result = val

            elif cur == "USD":
                # Phát hiện dạng "1.5k" hoặc "2k"
                if val < 10 and 'k' in orig_text:
                    val = val * 1000
                elif val < 100 and any(kw in orig_text for kw in ['k', 'nghìn', 'thousand']):
                    val = val * 1000
                # Nếu val vẫn rất lớn (vd 10,000,000 USD) → dữ liệu nhiễu → None
                if val > 50_000:
                    return None
                result = val * USD_TO_M_VND

            elif cur == "EUR":
                if val < 10 and 'k' in orig_text:
                    val = val * 1000
                elif val < 100 and any(kw in orig_text for kw in ['k', 'nghìn', 'thousand']):
                    val = val * 1000
                if val > 50_000:
                    return None
                result = val * EUR_TO_M_VND

            elif cur == "JPY":
                if any(kw in orig_text for kw in ["man", "lá", "la", "万"]):
                    result = val * MAN_TO_M_VND
                elif any(kw in orig_text for kw in ["sen", "sên"]):
                    result = (val * 1000) * YEN_TO_M_VND
                elif val >= 1_000:
                    result = val * YEN_TO_M_VND
                else:
                    result = val * MAN_TO_M_VND

            else:
                result = val

            # Clamp outlier: lương > MAX_SALARY_M_VND → dữ liệu nhiễu → None
            if result > MAX_SALARY_M_VND:
                return None

            return result

        # ── 5. Xác định min / max ───────────────────────────────────────────
        if len(numbers) == 1:
            m_val = convert_to_million(numbers[0], currency, original)
            if m_val is None:
                return (None, None, currency)
            return (float(m_val), float(m_val), currency)
        else:
            num1 = convert_to_million(numbers[0], currency, original)
            num2 = convert_to_million(numbers[1], currency, original)

            if num1 is None and num2 is None:
                return (None, None, currency)
            if num1 is None:
                return (float(num2), float(num2), currency)
            if num2 is None:
                return (float(num1), float(num1), currency)

            return (float(min(num1, num2)), float(max(num1, num2)), currency)

    parse_udf = udf(parse_salary, schema)

    return (
        df
        .withColumn("_salary_parsed", parse_udf(col("salary")))
        .withColumn("min_salary", col("_salary_parsed.min_salary"))
        .withColumn("max_salary", col("_salary_parsed.max_salary"))
        .withColumn("currency",   col("_salary_parsed.currency"))
        .drop("_salary_parsed")
    )