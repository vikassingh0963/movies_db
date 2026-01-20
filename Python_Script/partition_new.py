import subprocess
import psycopg2
import time
from datetime import datetime
start = time.time()
# Step 1: Connect to the database
conn = psycopg2.connect(
    host="localhost", dbname="movies", user="postgres", password="password"
)
cur = conn.cursor()

# Step 2: Get all partition names from hdfs_partition_log
cur.execute("SELECT partition_name FROM hdfs_partition_log")
db_partitions = set(row[0] for row in cur.fetchall())

# Step 3: Define all table names you want to check
tables = ["actor","director","movie","genre","languages"]
partition = ["Transformed_actor","Transformed_director","Transformed_movies","Transformed_genre","Transformed_language"]

# Set to hold all existing HDFS partitions across all tables
hdfs_partitions = set()

# Step 4: For each table, list folders under /partitions/{table}/
#for table in tables:
for i in partition:
    hdfs_path = f"/{i}"
    print(hdfs_path)
    try:
        hdfs_ls = subprocess.run(
            f'hdfs dfs -ls {hdfs_path}',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            shell=True  # Needed on Windows
        )
    except Exception as e:
        print(f"Error listing HDFS for {hdfs_path}: {e}")
        continue

    if hdfs_ls.returncode != 0:
        print(f"No directory or error for {hdfs_path}: {hdfs_ls.stderr.strip()}")
        continue

    for line in hdfs_ls.stdout.strip().splitlines():
        parts = line.strip().split()
        if len(parts) >= 8:
            path = parts[-1]
            partition_name = path.split("/")[-1]
            hdfs_partitions.add(partition_name)

# Step 5: Find missing partitions
missing_partitions = db_partitions - hdfs_partitions
print(f"Missing partitions: {missing_partitions}")

# Step 6: Delete from each table where partition_name is missing
if missing_partitions:
    partition_tuple = tuple(missing_partitions)

    for table in tables:
        cur.execute(
            f"DELETE FROM {table} WHERE partition_name IN %s", (partition_tuple,)
        )
        print(f"Deleted missing partitions from table: {table}")

    # Also delete from the log table
    cur.execute(
        "DELETE FROM hdfs_partition_log WHERE partition_name IN %s", (partition_tuple,)
    )
    print(f"Deleted from hdfs_partition_log")

conn.commit()
cur.close()
conn.close()

print("PARTITION CLEANUP COMPLETED")
print("Total Time:", time.time() - start, "seconds")
