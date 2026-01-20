from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_date, substring, year, col, when
)
from pyspark.sql.types import DoubleType

from pyspark.sql.functions import trim, col, lit, current_date, to_date
from datetime import datetime
import psycopg2
import traceback
import sys
start_time = datetime.now()
# Generate Partition Name
table_name = "movie"
date = start_time.strftime("%Y%m%d")  # Only date
partition_name = f"{table_name}_{date}"
hdfs_partition_path = f"/Transformed_movies/{partition_name}"

# 1. Start Spark Session
spark = SparkSession.builder.appName("Movie_Transform").getOrCreate()

# 2. Read CSV from HDFS
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", "|")
    .csv("hdfs://localhost:9000/demo_output/movies_20250907.csv")
)

# 3. Cast numeric columns
df = df.withColumn("imdb_rating", col("imdb_rating").cast(DoubleType()))
df = df.withColumn("profit", col("profit").cast(DoubleType()))
df = df.withColumn("budget_in_crores", col("budget_in_crores").cast(DoubleType()))

# 4. Handle release_date safely
df = df.withColumn("release_date", to_date("release_date", "dd-MM-yyyy"))
df = df.withColumn("release_year", year("release_date"))

# 5. ROI with safe division (avoid divide by zero)
df = df.withColumn(
    "roi",
    when(col("budget_in_crores").isNotNull() & (col("budget_in_crores") != 0),
         (col("profit") / col("budget_in_crores")) * 100
    ).otherwise(None)
)

# 6. Rating categories
df = df.withColumn(
    "rating_category",
    when(col("imdb_rating") >= 8, "Excellent")
    .when(col("imdb_rating") >= 6, "Good")
    .otherwise("Average")
)
df = df.withColumn("partition_name", lit(partition_name))

# 7. file_name → file_date (safe parse)
df = df.withColumn(
    "file_date",
    to_date(
        substring(col("file_name"), 8, 8),   # extract 20250907
        "yyyyMMdd"
    )
)

# 8. Split into valid & rejected rows
valid_df = df.filter(col("release_date").isNotNull() & col("roi").isNotNull())
rejected_df = df.filter(col("release_date").isNull() | col("roi").isNull())

output_path = f"hdfs://localhost:9000/Transformed_movies/{partition_name}"

# 9. Save valid rows to HDFS (partitioned by file_date)

valid_df.write.mode("overwrite").option("header", True).csv(output_path)
#valid_df.write.mode("overwrite").option("header", True).partitionBy("file_date").csv("hdfs://localhost:9000/Transformed_movies/")

# 10. Save rejected rows separately
rejected_df.write.mode("overwrite").option("header", True).csv(
    "hdfs://localhost:9000/Rejected_movies/"
)

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

# 11. Stop Spark
spark.stop()
