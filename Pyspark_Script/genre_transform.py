from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_date, substring, year, col, when
)
from pyspark.sql.functions import trim, col, lit, current_date, to_date
from datetime import datetime
import psycopg2
import traceback
import sys
start_time = datetime.now()
# Generate Partition Name
table_name = "genre"
date = start_time.strftime("%Y%m%d")  # Only date
partition_name = f"{table_name}_{date}"
hdfs_partition_path = f"/Transformed_genre/{partition_name}"

# 1. Start Spark Session
spark = SparkSession.builder.appName("Genre_Transform").getOrCreate()

# 2. Read CSV from HDFS
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", "|")
    .csv("hdfs://localhost:9000/data_output/genre/genere_20250907.csv")
)

# 3. file_name → file_date (safe parse)
df = df.withColumn(
    "file_date",
    to_date(
        substring(col("file_name"), 8, 8),   # extract 20250907
        "yyyyMMdd"
    )
)
df = df.withColumn("partition_name", lit(partition_name))

# 4. Save rows to HDFS (partitioned by file_date)

output_path = f"hdfs://localhost:9000/Transformed_genre/{partition_name}"
 
df.write.mode("overwrite").option("header", True).csv(output_path)
#df.write.mode("overwrite").option("header", True).partitionBy("file_date").csv("hdfs://localhost:9000/Transformed_genre/")

# Log to hdfs_partition_log table

conn = psycopg2.connect(dbname="movies", user="postgres", password="password", host="192.168.1.3")
cur = conn.cursor()
date = datetime.today().date()
cur.execute("""
    INSERT INTO hdfs_partition_log (table_name, partition_name, hdfs_path, date)
    VALUES (%s, %s, %s, %s)
""", (table_name, partition_name, hdfs_partition_path, date))
conn.commit()
cur.close()
conn.close()

# 5. Stop Spark
spark.stop()