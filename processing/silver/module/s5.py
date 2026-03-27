# s5.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import re

# Boundary Unicode-aware
_B  = r'(?<![a-zA-Z0-9\u00C0-\u024F\u1E00-\u1EFF])'
_BE = r'(?![a-zA-Z0-9\u00C0-\u024F\u1E00-\u1EFF])'

def process(df: DataFrame, level_map: dict) -> DataFrame:
    """
    Trích xuất cấp độ (level) từ title + experience.

    Sửa lỗi: `if any(kw in text ...)` bắt nhầm chuỗi con:
      - "intern" match trong "international"
      - "lead"   match trong "leading", "uploaded"
      - "senior" match trong "senior" OK, nhưng nhất quán dùng regex
    Giải pháp: dùng regex với Unicode-aware lookaround.
    """

    # Thứ tự ưu tiên: Manager > Lead > Senior > Middle > Junior > Fresher > Intern
    PRIORITY = ["Manager", "Lead", "Senior", "Middle", "Junior", "Fresher", "Intern"]

    # Pre-compile patterns cho mỗi level
    compiled_level_patterns: dict[str, list[re.Pattern]] = {}
    for level in PRIORITY:
        keywords = level_map.get(level, [])
        compiled_level_patterns[level] = [
            re.compile(_B + re.escape(kw.lower()) + _BE)
            for kw in keywords
        ]

    def extract_level(title: str, experience: str) -> str:
        if not title:
            return None

        text = (title + " " + (experience or "")).lower()

        for level in PRIORITY:
            for pattern in compiled_level_patterns[level]:
                if pattern.search(text):
                    return level

        return None

    level_udf = udf(extract_level, StringType())
    return df.withColumn("level", level_udf(col("title"), col("experience")))