# verify_data.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
from pathlib import Path
import sys

# Initialize Spark
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("VerifyData") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .config("spark.sql.sources.parallelPartitionDiscovery.threshold", "1000") \
    .config("spark.sql.debug.maxToStringFields", "100") \
    .getOrCreate()

# Reduce logging noise
spark.sparkContext.setLogLevel("ERROR")

BASE_DIR = Path("/home/eric_s/dev_work/github.com/EricS-Arbitr/retro-hunt-lab")
DELTA_BASE = BASE_DIR / "data2"

print("=" * 60)
print("DELTA LAKE DATA INVENTORY")
print("=" * 60)

# Find all Delta tables
delta_tables = []
for root, dirs, files in os.walk(DELTA_BASE):
    if "_delta_log" in dirs:
        table_path = root
        table_name = os.path.basename(table_path)
        delta_tables.append((table_name, table_path))

if not delta_tables:
    print("⚠ No Delta tables found!")
    print("Please complete LAB-163 to load historical data.")
    exit(1)

# Analyze each table
for table_name, table_path in delta_tables:
    print(f"\n{'=' * 60}")
    print(f"Table: {table_name}")
    print(f"Path: {table_path}")
    print(f"{'=' * 60}")
    
    try:
        # Try reading as Delta first
        try:
            df = spark.read \
                .option("mergeSchema", "true") \
                .format("delta") \
                .load(table_path)
        except Exception as delta_error:
            # Fallback to reading parquet files directly
            print(f"⚠ Delta read failed (known Spark 3.5.x bug), reading parquet directly...")
            parquet_files = []
            for root, dirs, files in os.walk(table_path):
                if "_delta_log" not in root:
                    for f in files:
                        if f.endswith(".parquet"):
                            parquet_files.append(os.path.join(root, f))

            if parquet_files:
                df = spark.read.parquet(*parquet_files)
                print(f"✓ Read {len(parquet_files)} parquet file(s) directly")
            else:
                raise Exception("No parquet files found")

        record_count = df.count()
        print(f"Total Records: {record_count:,}")

        # Check for partitions
        if "year" in df.columns and "month" in df.columns:
            date_range = df.agg(
                min(concat_ws("-", col("year"), col("month"), col("day"))).alias("earliest"),
                max(concat_ws("-", col("year"), col("month"), col("day"))).alias("latest")
            ).collect()[0]

            print(f"Date Range: {date_range['earliest']} to {date_range['latest']}")

            partition_count = df.select("year", "month", "day").distinct().count()
            print(f"Partitions: {partition_count}")

        # Show column count
        print(f"Columns: {len(df.columns)}")

        # Sample data
        print("\nSample Record:")
        df.limit(1).show(1, truncate=False, vertical=True)

    except Exception as e:
        print(f"✗ Error reading table: {e}")

print(f"\n{'=' * 60}")
print(f"✓ Found {len(delta_tables)} Delta tables")
print("✓ Data verification complete")
print(f"{'=' * 60}")
