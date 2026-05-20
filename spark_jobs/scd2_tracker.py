from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import os
import warnings
warnings.filterwarnings("ignore")

# Create Spark Session
builder = (
    SparkSession.builder
    .appName("SCD Type 2 Tracker")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read Silver Layer
source_df = spark.read.format("delta").load("delta/silver/retail_sales")

# Enable automatic schema evolution
spark.conf.set(
    "spark.databricks.delta.schema.autoMerge.enabled",
    "true"
)

# Add SCD2 columns
source_df = (
    source_df
    .withColumn("start_date", current_date())
    .withColumn("end_date", lit(None).cast("date"))
    .withColumn("is_active", lit(True).cast("boolean"))
)

# SCD2 table path
scd2_path = "delta/gold/retail_history"

# Initial Load
if not os.path.exists(scd2_path):

    (
        source_df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(scd2_path)
    )

    print("Initial SCD2 Load Completed!")

    

# Incremental SCD2 Logic
else:

    spark.catalog.clearCache()


    delta_table = DeltaTable.forPath(
        spark,
        scd2_path
    )

    # Expire old records if changes detected
    (
        delta_table.alias("target")
        .merge(
            source_df.alias("source"),
            """
            target.transaction_id = source.transaction_id
            AND target.is_active = true
            """
        )
        .whenMatchedUpdate(
            condition="""
                target.product_category <> source.product_category
                OR target.quantity <> source.quantity
                OR target.total_amount <> source.total_amount
            """,
            set={
                "is_active": "false",
                "end_date": "current_date()"
            }
        )
        .execute()
    )

    # Insert new active records
    (
        delta_table.alias("target")
        .merge(
            source_df.alias("source"),
            """
            target.transaction_id = source.transaction_id
            AND target.is_active = true
            """
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("SCD Type 2 MERGE Completed!")

# Read Final SCD2 Table
final_df = spark.read.format("delta").load(scd2_path)

# Show Output
print("\nSCD2 History Table:")
final_df.show(3, truncate=False, vertical=True)

spark.stop()