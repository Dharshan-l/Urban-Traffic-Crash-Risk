from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# -----------------------------------
# 1. CREATE SPARK SESSION
# -----------------------------------
spark = SparkSession.builder \
    .appName("Gold Layer Final") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# -----------------------------------
# 2. READ SILVER DATA
# -----------------------------------
silver_path = "data_lake/silver/traffic_crashes"

print("Reading Silver data...")

df = spark.read.parquet(silver_path)

print("Silver data loaded")

# -----------------------------------
# 3. SHOW COLUMNS (DEBUG)
# -----------------------------------
print("Columns in dataset:")
print(df.columns)

# -----------------------------------
# 4. CRASH COUNT BY STREET (FIXED)
# -----------------------------------
crash_by_area = df.groupBy("street_name") \
    .agg(count("*").alias("crash_count"))

# -----------------------------------
# 5. MONTHLY TREND
# -----------------------------------
monthly_trend = df.groupBy("year", "month") \
    .agg(count("*").alias("monthly_crashes"))

# -----------------------------------
# 6. HIGH RISK ROADS (TOP 10)
# -----------------------------------
high_risk = df.groupBy("street_name") \
    .agg(count("*").alias("crash_count")) \
    .orderBy(col("crash_count").desc()) \
    .limit(10)

# -----------------------------------
# 7. SAVE GOLD DATA
# -----------------------------------
gold_path = "data_lake/gold"

crash_by_area.write.mode("overwrite").parquet(f"{gold_path}/crash_by_area")
monthly_trend.write.mode("overwrite").parquet(f"{gold_path}/monthly_trend")
high_risk.write.mode("overwrite").parquet(f"{gold_path}/high_risk")

print("🎉 Gold layer created successfully!")

# -----------------------------------
# 8. STOP SPARK
# -----------------------------------
spark.stop()