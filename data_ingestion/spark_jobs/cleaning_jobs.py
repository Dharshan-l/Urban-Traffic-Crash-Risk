from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month
import os

# -----------------------------------
# 1. CREATE SPARK SESSION
# -----------------------------------
spark = SparkSession.builder \
    .appName("Silver Layer Final with Incremental") \
    .getOrCreate()

# -----------------------------------
# 2. PATHS
# -----------------------------------
bronze_path = "data_lake/bronze/traffic_crashes"
silver_path = "data_lake/silver/traffic_crashes"
last_file = "data_ingestion/last_processed.txt"

print("Reading data...")

# -----------------------------------
# 3. READ JSON FILES
# -----------------------------------
df = spark.read \
    .option("multiline", "true") \
    .option("recursiveFileLookup", "true") \
    .json(bronze_path)

print("Data loaded")

# -----------------------------------
# 4. DEBUG (SEE COLUMNS)
# -----------------------------------
print("Columns in dataset:")
print(df.columns)

# -----------------------------------
# 5. CONVERT DATE FIRST (IMPORTANT)
# -----------------------------------
df = df.withColumn("crash_date", to_timestamp(col("crash_date")))

# -----------------------------------
# 6. LOAD LAST PROCESSED DATE
# -----------------------------------
if os.path.exists(last_file):
    with open(last_file, "r") as f:
        last_date = f.read().strip()
else:
    last_date = "2020-01-01"

print("Last processed date:", last_date)

# -----------------------------------
# 7. APPLY INCREMENTAL FILTER
# -----------------------------------
df = df.filter(col("crash_date") > last_date)

# -----------------------------------
# 8. DATA CLEANING
# -----------------------------------

# Remove nulls
df = df.dropna(subset=["crash_record_id", "crash_date"])

# Remove duplicates
df = df.dropDuplicates(["crash_record_id"])

# Add year & month
df = df.withColumn("year", year(col("crash_date"))) \
       .withColumn("month", month(col("crash_date")))

# -----------------------------------
# 9. WRITE TO SILVER LAYER
# -----------------------------------
df.write \
    .mode("append") \
    .partitionBy("year", "month","street_name") \
    .parquet(silver_path)

print("✅ Silver layer created successfully!")

# -----------------------------------
# 10. UPDATE LAST PROCESSED DATE
# -----------------------------------
max_date_row = df.selectExpr("max(crash_date)").collect()[0]
max_date = max_date_row[0]

if max_date:
    with open(last_file, "w") as f:
        f.write(str(max_date))

    print("Updated last processed date:", max_date)
else:
    print("No new data processed")
# -----------------------------------
# DATA QUALITY CHECKS
# -----------------------------------

print("🔍 Running Data Quality Checks...")

# 1. NULL CHECK
null_count = df.filter(
    col("crash_record_id").isNull() | col("crash_date").isNull()
).count()

print("Null records:", null_count)

# 2. DUPLICATE CHECK
duplicate_count = df.groupBy("crash_record_id").count() \
    .filter(col("count") > 1).count()

print("Duplicate records:", duplicate_count)

# 3. DATE VALIDATION
invalid_date_count = df.filter(col("crash_date").isNull()).count()

print("Invalid date records:", invalid_date_count)

# 4. LAT/LONG RANGE CHECK (optional but strong)
invalid_location = df.filter(
    (col("latitude") < -90) | (col("latitude") > 90) |
    (col("longitude") < -180) | (col("longitude") > 180)
).count()

print("Invalid location records:", invalid_location)

print("✅ Data Quality Checks Completed")
# -----------------------------------
# 11. STOP SPARK
# -----------------------------------
spark.stop()