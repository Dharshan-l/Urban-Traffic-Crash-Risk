from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp

spark = SparkSession.builder \
    .appName("SCD Type 2") \
    .getOrCreate()

silver_path = "data_lake/silver/traffic_crashes"
dim_path = "data_lake/warehouse/dim_location_scd"

# -----------------------------------
# LOAD NEW DATA
# -----------------------------------
df = spark.read.parquet(silver_path)

new_dim = df.select("street_name", "latitude", "longitude").dropDuplicates()

# -----------------------------------
# CHECK IF OLD TABLE EXISTS
# -----------------------------------
import os

if os.path.exists(dim_path):

    old_dim = spark.read.parquet(dim_path)

    # Join to find changed records
    joined = new_dim.join(old_dim, "street_name", "left")

    # New records (not existing)
    new_records = joined.filter(old_dim.street_name.isNull())

    # Existing records → expire old
    updated_old = old_dim.withColumn("is_current", lit(False)) \
        .withColumn("end_date", current_timestamp())

    # Add new versions
    new_records = new_records \
        .withColumn("effective_date", current_timestamp()) \
        .withColumn("end_date", lit(None)) \
        .withColumn("is_current", lit(True))

    final_dim = updated_old.union(new_records)

else:
    # First time load
    final_dim = new_dim \
        .withColumn("effective_date", current_timestamp()) \
        .withColumn("end_date", lit(None)) \
        .withColumn("is_current", lit(True))

# -----------------------------------
# WRITE
# -----------------------------------
final_dim.write.mode("overwrite").parquet(dim_path)

print("✅ SCD Type 2 applied successfully!")

spark.stop()