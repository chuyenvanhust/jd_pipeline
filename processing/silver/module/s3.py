# s3.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import re

# Boundary Unicode-aware (giống s0, s5, s6)
_B  = r'(?<![a-zA-Z0-9\u00C0-\u024F\u1E00-\u1EFF])'
_BE = r'(?![a-zA-Z0-9\u00C0-\u024F\u1E00-\u1EFF])'

def process(df: DataFrame, location_map: dict) -> DataFrame:
   
    # ── Tiền xử lý: flatten + sort dài → ngắn + pre-compile ────────────────
    sorted_keywords = []
    for city, keywords in location_map.items():
        for kw in keywords:
            pattern = re.compile(_B + re.escape(kw.lower()) + _BE)
            sorted_keywords.append((pattern, city))

    # Sắp xếp theo độ dài keyword gốc giảm dần
    sorted_keywords.sort(key=lambda x: len(x[0].pattern), reverse=True)

    def normalize_location(raw: str) -> str:
        if not raw:
            return "Khác"

        text = raw.lower().strip()

        for pattern, standardized_city in sorted_keywords:
            if pattern.search(text):
                return standardized_city

        return "Khác"

    loc_udf = udf(normalize_location, StringType())
    return df.withColumn("city", loc_udf(col("location")))
