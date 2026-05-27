from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, lit
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
delta_base_path = os.getenv("DELTA_BASE_PATH", "delta")
silver_path = os.path.join(delta_base_path, "silver", "retail_sales")
scd2_path = os.path.join(delta_base_path, "gold", "retail_history")

source_df = spark.read.format("delta").load(silver_path)

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

    change_condition = """
        target.product_category <> source.product_category
        OR target.quantity <> source.quantity
        OR target.price_per_unit <> source.price_per_unit
        OR target.total_amount <> source.total_amount
    """

    # Expire old active records if changes are detected
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
            condition=change_condition,
            set={
                "is_active": "false",
                "end_date": "current_date()"
            }
        )
        .execute()
    )

    current_active_df = (
        spark.read
        .format("delta")
        .load(scd2_path)
        .filter(col("is_active") == lit(True))
        .select("transaction_id")
    )

    new_active_df = (
        source_df.alias("source")
        .join(
            current_active_df.alias("target"),
            on="transaction_id",
            how="left_anti"
        )
    )

    # Insert brand-new records and changed records as new active versions
    (
        new_active_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(scd2_path)
    )

    print("SCD Type 2 MERGE Completed!")

# Read Final SCD2 Table
final_df = spark.read.format("delta").load(scd2_path)

# Show Output
print("\nSCD2 History Table:")
final_df.show(3, truncate=False, vertical=True)

spark.stop()
