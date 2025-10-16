from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
from pathlib import Path
import sys


# Define base paths
BASE_DIR = Path("/home/eric_s/dev_work/github.com/EricS-Arbitr/retro-hunt-lab")
DELTA_BASE = BASE_DIR / "data2"
HUNT_LIBRARY = BASE_DIR / "end_to_end/hunt_library"
RESULTS_DIR = BASE_DIR / "end_to_end/hunt_results"

print("=" * 70)
print("RETROSPECTIVE HUNT LAB - ENVIRONMENT SETUP")
print("=" * 70)

# Step 1: Create directories
print("\n[1/5] Creating directory structure...")
for directory in [DELTA_BASE, HUNT_LIBRARY, RESULTS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)
    print(f"  ✓ Created: {directory}")

# Step 2: Check PySpark version
print("\n[2/5] Checking PySpark version...")
import pyspark
pyspark_version = pyspark.__version__
spark_major = int(pyspark_version.split('.')[0])
spark_major_minor = '.'.join(pyspark_version.split('.')[:2])
print(f"  PySpark version: {pyspark_version}")

if spark_major >= 4:
    print("\n" + "!" * 70)
    print("ERROR: PySpark 4.x requires different Delta Lake setup")
    print("!" * 70)
    print("\nRecommended: Downgrade to PySpark 3.5.3")
    print("  pip uninstall pyspark -y")
    print("  pip install pyspark==3.5.3 delta-spark==3.1.0")
    sys.exit(1)

# Step 3: Configure Delta Lake with CORRECT Scala version
print("\n[3/5] Configuring Delta Lake...")

# CRITICAL: PySpark 3.5.x uses Scala 2.12
# Must use delta-spark_2.12 (Scala 2.12 version)
delta_config = {
    "3.5": {
        "package": "io.delta:delta-spark_2.12:3.1.0",
        "version": "3.1.0",
        "scala": "2.12"
    },
    "3.4": {
        "package": "io.delta:delta-spark_2.13:3.0.0",
        "version": "3.0.0",
        "scala": "2.13"
    },
    "3.3": {
        "package": "io.delta:delta-core_2.12:2.4.0",
        "version": "2.4.0",
        "scala": "2.12"
    }
}

config = delta_config.get(spark_major_minor, delta_config["3.5"])
delta_package = config["package"]
delta_version = config["version"]
scala_version = config["scala"]

print(f"  Delta package: {delta_package}")
print(f"  Delta version: {delta_version}")
print(f"  Scala version: {scala_version}")

# Verify delta-spark is installed
try:
    import delta
    print(f"  ✓ Delta Python package found")
except ImportError:
    print(f"  Installing delta-spark...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", f"delta-spark=={delta_version}"])
    print(f"  ✓ Delta Python package installed")

# Step 4: Initialize Spark
print("\n[4/5] Initializing Spark with Delta Lake...")

# Stop any existing sessions
try:
    existing_spark = SparkSession.getActiveSession()
    if existing_spark:
        print("  Stopping existing Spark session...")
        existing_spark.stop()
except:
    pass

def create_spark_session(app_name="RetroHunt"):
    """Create Spark session with Delta Lake support"""
    
    print(f"  Downloading JARs for {delta_package}...")
    
    builder = SparkSession.builder \
        .master("local[*]") \
        .appName(app_name)
    
    # Add Delta Lake packages
    builder = builder.config("spark.jars.packages", delta_package)
    
    # Add Delta extensions
    builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    # Memory settings
    builder = builder \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.sql.debug.maxToStringFields", "100")

    # Create session
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")  # Reduce noise
    
    return spark

try:
    spark = create_spark_session()
    print(f"  ✓ Spark {spark.version} initialized")
    
    # Verify Scala version matches
    scala_ver = spark.sparkContext._jvm.scala.util.Properties.versionString()
    print(f"  Spark Scala version: {scala_ver}")
    
    if scala_version not in scala_ver:
        print(f"  ⚠ Warning: Scala version mismatch!")
        print(f"     Expected Scala {scala_version}, found {scala_ver}")
        
except Exception as e:
    print(f"  ✗ Failed to initialize Spark: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Step 5: Test Delta Lake
print("\n[5/5] Testing Delta Lake functionality...")
test_path = str(DELTA_BASE / "_test")

try:
    # Create test data
    print("  [Test 1/4] Creating test DataFrame...")
    test_data = [(1, "alice", "2024-10-14"), (2, "bob", "2024-10-15")]
    test_df = spark.createDataFrame(test_data, ["id", "name", "date"])
    print(f"  ✓ Created DataFrame with {test_df.count()} rows")
    
    # Write as Delta
    print("  [Test 2/4] Writing Delta table...")
    test_df.write.format("delta").mode("overwrite").save(test_path)
    print(f"  ✓ Wrote Delta table to {test_path}")
    
    # Read as Delta
    print("  [Test 3/4] Reading Delta table...")
    test_read = spark.read.format("delta").load(test_path)
    row_count = test_read.count()
    print(f"  ✓ Read Delta table ({row_count} rows)")
    
    if row_count != 2:
        raise Exception(f"Row count mismatch: expected 2, got {row_count}")
    
    # Test Delta features
    print("  [Test 4/4] Testing Delta Lake features...")
    from delta.tables import DeltaTable
    dt = DeltaTable.forPath(spark, test_path)
    history = dt.history(1).collect()
    print(f"  ✓ Time travel works ({len(history)} version(s))")
    
    # Show sample data
    print("\n  Sample data from Delta table:")
    test_read.show(truncate=False)
    
    # Cleanup
    import shutil
    shutil.rmtree(test_path, ignore_errors=True)
    
    print("\n  ✓ ALL DELTA LAKE TESTS PASSED")
    delta_enabled = True
    
except Exception as e:
    print(f"\n  ✗ Delta Lake test failed!")
    print(f"  Error: {e}")
    delta_enabled = False
    
    import traceback
    print("\n  Full traceback:")
    traceback.print_exc()
    
    print("\n" + "!" * 70)
    print("TROUBLESHOOTING REQUIRED")
    print("!" * 70)
    print("\nYour current setup:")
    print(f"  PySpark version: {pyspark_version}")
    print(f"  Delta package:   {delta_package}")
    print(f"  Scala version:   {scala_version}")
    print("\nTry this fix:")
    print("  1. Clear all caches:")
    print("     rm -rf ~/.ivy2/cache ~/.ivy2/jars")
    print("  2. Reinstall packages:")
    print("     pip uninstall pyspark delta-spark -y")
    print("     pip install pyspark==3.5.3 delta-spark==3.1.0")
    print("  3. Restart Python completely")
    print("  4. Run this script again")
    print("!" * 70)
    
    spark.stop()
    sys.exit(1)

# Create configuration file
print("\n[6/6] Creating configuration file...")
config = {
    'delta_enabled': delta_enabled,
    'delta_base': str(DELTA_BASE),
    'hunt_library': str(HUNT_LIBRARY),
    'results_dir': str(RESULTS_DIR),
    'spark_version': spark.version,
    'pyspark_version': pyspark_version,
    'delta_package': delta_package,
    'delta_version': delta_version,
    'scala_version': scala_version,
    'format': 'delta'
}

import json
config_path = BASE_DIR / "hunt_config.json"
with open(config_path, 'w') as f:
    json.dump(config, f, indent=2)

print(f"  ✓ Configuration saved to: {config_path}")

# Print summary
print("\n" + "=" * 70)
print("✓ ENVIRONMENT SETUP COMPLETE")
print("=" * 70)
print(f"  PySpark:         {pyspark_version}")
print(f"  Spark:           {spark.version}")
print(f"  Delta Lake:      {delta_version}")
print(f"  Scala:           {scala_version}")
print(f"  Delta Base:      {DELTA_BASE}")
print(f"  Hunt Library:    {HUNT_LIBRARY}")
print(f"  Results Dir:     {RESULTS_DIR}")
print("=" * 70)

spark.stop()

print("\n✓ Ready to start the lab!")