#silver_process.py
import config

from pyspark.sql import SparkSession

from module import s0, s1, s2, s3, s4, s5, s6, s7

""" 
    Cần có được :
        0.is_it (IT job hay không) ->điều kiện tiên quyết
        1.company
        2.min ,max salary (triệu VND)
        3.location (tỉnh)
        4.số năm kinh nghiệm 
        5.level
        6.domain
        7.skill (ngôn ngữ lập trình ,kĩ năng chuyên môn)



"""
def main():
    spark = SparkSession.builder \
        .appName("topcv-silver-process") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # silver_process.py

    # 1. Load Data
    df = spark.read.schema(config.schema).parquet(config.INPUT_PATH)
    print(f" Raw input: {df.count()} rows")

    # 2. Pipeline Processing
    #
    # Thứ tự mới:
    #   s6 (domain detection) chạy TRƯỚC s0 (IT filter)
    #   vì s0 giờ dựa vào cột `domain` để phân loại IT/non-IT.
    #
    #   Luồng đầy đủ:
    #     s6  → phát hiện domain từ title/tags/HTML (toàn bộ data thô)
    #     s0  → gán is_it = True nếu domain ≠ ["Other"]
    #     s1  → chuẩn hoá tên công ty
    #     s2  → parse & quy đổi lương
    #     s3  → chuẩn hoá địa điểm → city
    #     s4  → trích xuất số năm kinh nghiệm
    #     s5  → trích xuất cấp độ (Senior/Junior/...)
    #     s7  → trích xuất kỹ năng theo domain (cần cột domain từ s6)

    df_with_domain = df.transform(
        lambda d: s6.process(d, config.DOMAIN_MAP)
    )

    df_s0 = df_with_domain.transform(s0.process)
    print(
        f" s0 - IT filter: "
        f"{df_s0.filter('is_it = true').count()} IT "
        f"/ {df_s0.count()} total"
    )

    df_gold = (
        df_s0
        .filter("is_it = true")
        .transform(s1.process)
        .transform(lambda d: s2.process(d, config.SALARY_UNIT_MAP))
        .transform(lambda d: s3.process(d, config.LOCATION_MAP))
        .transform(s4.process)
        .transform(lambda d: s5.process(d, config.LEVEL_MAP))
        # s6 đã chạy trên toàn bộ data ở trên (df_with_domain),
        # không cần chạy lại — cột `domain` đã có sẵn.
        .transform(lambda d: s7.process(d, config.SKILL_DICTIONARY))
    )

    total = df_gold.count()
    print(f" Output final: {total} rows")
    print(f"   - Có salary: {df_gold.filter('min_salary is not null').count()}")
    print(f"   - Có level:  {df_gold.filter('level is not null').count()}")
    print(f"   - Có domain: {df_gold.filter('domain is not null').count()}")
    print(f"   - Có skill:  {df_gold.filter('skills is not null').count()}")

    # 3. Save 
    df_gold.write.mode("overwrite").parquet(config.OUTPUT_PATH)
    print(f"Successfully processed to: {config.OUTPUT_PATH}")
    

if __name__ == "__main__":
    main()
