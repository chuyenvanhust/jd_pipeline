#s1.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, regexp_replace

def process(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "company",
        trim(regexp_replace(col("company"), r"\s+", " "))
    )