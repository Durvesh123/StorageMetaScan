#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql.functions import col, udf
from pyspark.sql import functions as F
import os
import pwd
from datetime import datetime
import sys

# 1️⃣ Initialize Spark
spark = SparkSession.builder \
    .appName("VMFileMetadataScanner") \
    .config("spark.jars", "/opt/spark/jars/mysql-connector-j-8.1.0.jar") \
    .getOrCreate()

# 2️⃣ Define schema for DataFrame
schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("file_path", StringType(), True),
    StructField("owner", StringType(), True),
    StructField("size_bytes", LongType(), True),
    StructField("modification_time", TimestampType(), True)
])

# 3️⃣ Scan VM recursively
root_dir = "/"  # Change if you want to scan a specific directory
data = []

print("Scanning files, this may take a while...")
for dirpath, dirnames, filenames in os.walk(root_dir):
    for f in filenames:
        try:
            full_path = os.path.join(dirpath, f)
            stat = os.stat(full_path)
            owner = pwd.getpwuid(stat.st_uid).pw_name
            size = stat.st_size
            # Convert timestamp to naive UTC (MySQL compatible)
            mtime = datetime.utcfromtimestamp(stat.st_mtime)
            data.append((f, full_path, owner, size, mtime))
        except Exception:
            continue  # skip files without permissions

# 4️⃣ Create DataFrame
df = spark.createDataFrame(data, schema)

# Optional: remove any zero timestamps (optional safety)
df = df.filter(col("modification_time") > F.lit(datetime(1970, 1, 1)))

# Optional: repartition to reduce huge task sizes
df = df.repartition(10)

# 5️⃣ Save Parquet (backup)
output_dir = f"/root/metadata/scan_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
df.write.mode("overwrite").parquet(output_dir)
print(f"Parquet backup saved to {output_dir}")

# 6️⃣ Load into MySQL
mysql_url = "jdbc:mysql://localhost:3306/storage_metadata"
mysql_properties = {
    "user": "sparkuser",
    "password": "SparkStrongPass123!",
    "driver": "com.mysql.cj.jdbc.Driver"
}

try:
    df.write.jdbc(url=mysql_url, table="file_metadata", mode="overwrite", properties=mysql_properties)
    print("Data successfully written to MySQL table 'file_metadata'.")
except Exception as e:
    print("Error writing to MySQL:", e)
    sys.exit(1)

spark.stop()

