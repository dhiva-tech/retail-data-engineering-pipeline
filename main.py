import os

print("\n========== STARTING DATA PIPELINE ==========\n")

# Bronze Layer
print("\nRunning Bronze Ingestion...\n")
os.system("python spark_jobs/bronze_ingestion.py")

# Incremental Merge
print("\nRunning Incremental Merge...\n")
os.system("python spark_jobs/bronze_incremental_merge.py")

# Silver Layer
print("\nRunning Silver Transformation...\n")
os.system("python spark_jobs/silver_transformation.py")

# SCD Type 2
print("\nRunning SCD Type 2 Tracking...\n")
os.system("python spark_jobs/scd2_tracker.py")

# Gold Layer
print("\nRunning Gold Star Schema...\n")
os.system("python spark_jobs/gold_star_schema.py")

print("\n========== PIPELINE COMPLETED ==========\n")