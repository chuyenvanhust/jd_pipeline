# s0.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, size, array_contains
from pyspark.sql.types import BooleanType


def process(df: DataFrame) -> DataFrame:
    """
    Phân loại IT job dựa vào cột `domain` (output của s6).

    Logic:
      - domain là ArrayType(StringType()), vd ["Backend Developer", "DevOps Engineer"]
      - Nếu array KHÔNG chứa "Other" HOẶC có nhiều hơn 1 phần tử → là IT job
      - Nếu domain = ["Other"] (chỉ có Other, không tìm được domain nào) → không phải IT

    Lý do dùng domain thay vì keyword list thủ công (cách cũ):
      - Nhất quán: cùng logic với s6, không bị drift giữa 2 bộ keyword.
      - Bao quát hơn: s6 quét title + tags + HTML, rộng hơn chỉ quét title.
      - Dễ bảo trì: chỉ cần cập nhật DOMAIN_MAP ở config, s0 tự được hưởng lợi.

    Yêu cầu:
      DataFrame đầu vào phải đã có cột `domain` → chạy s6 TRƯỚC s0.

    Trả về:
      DataFrame có thêm cột `is_it` (BooleanType).
    """

    is_it_udf = udf(
        lambda domains: (
            domains is not None
            and len(domains) > 0
            and not (len(domains) == 1 and domains[0] == "Other")
        ),
        BooleanType(),
    )

    return df.withColumn("is_it", is_it_udf(col("domain")))