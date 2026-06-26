from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id

# -----------------------------------
# 1. CREATE SPARK SESSION
# -----------------------------------
spark = SparkSession.builder \
    .appName("Star Schema Implementation") \
    .getOrCreate()

silver_path = "data_lake/silver/traffic_crashes"
warehouse_path = "data_lake/warehouse"

df = spark.read.parquet(silver_path)

print("✅ Silver data loaded")

# -----------------------------------
# 2. DIMENSION TABLES
# -----------------------------------

# 📅 dim_time
dim_time = df.select("year", "month").dropDuplicates() \
    .withColumn("time_id", monotonically_increasing_id())

# 📍 dim_location
dim_location = df.select("street_name", "latitude", "longitude").dropDuplicates() \
    .withColumn("location_id", monotonically_increasing_id())

# 🌦️ dim_weather
dim_weather = df.select("weather_condition", "lighting_condition").dropDuplicates() \
    .withColumn("weather_id", monotonically_increasing_id())

# 🚗 dim_crash_type
dim_crash_type = df.select("crash_type", "damage").dropDuplicates() \
    .withColumn("crash_type_id", monotonically_increasing_id())

# -----------------------------------
# 3. FACT TABLE
# -----------------------------------

fact = df \
    .join(dim_time, ["year", "month"], "left") \
    .join(dim_location, ["street_name", "latitude", "longitude"], "left") \
    .join(dim_weather, ["weather_condition", "lighting_condition"], "left") \
    .join(dim_crash_type, ["crash_type", "damage"], "left") \
    .select(
        "time_id",
        "location_id",
        "weather_id",
        "crash_type_id"
    )

# -----------------------------------
# 4. SAVE TABLES
# -----------------------------------

dim_time.write.mode("overwrite").parquet(f"{warehouse_path}/dim_time")
dim_location.write.mode("overwrite").parquet(f"{warehouse_path}/dim_location")
dim_weather.write.mode("overwrite").parquet(f"{warehouse_path}/dim_weather")
dim_crash_type.write.mode("overwrite").parquet(f"{warehouse_path}/dim_crash_type")

fact.write.mode("overwrite").parquet(f"{warehouse_path}/fact_traffic_crashes")

print("🎉 Star Schema Created Successfully!")

spark.stop()