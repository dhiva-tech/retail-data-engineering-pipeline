from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import os
import warnings
warnings.filterwarnings("ignore")

# Create Spark Session
builder = (
    SparkSession.builder
    .appName("Bronze Incremental Load")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Bronze path
delta_base_path = os.getenv("DELTA_BASE_PATH", "delta")
bronze_path = os.path.join(delta_base_path, "bronze", "retail_sales")
incremental_file_path = "data/retail_sales_incremental.csv"
source_file_path = (
    incremental_file_path
    if os.path.exists(incremental_file_path)
    else "data/retail_sales.csv"
)

# Read CSV
df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(source_file_path)
)

# Clean column names
clean_columns = [
    col.strip()
       .lower()
       .replace(" ", "_")
       .replace("/", "_")
    for col in df.columns
]

df = df.toDF(*clean_columns)

# Select required columns
required_columns = [
    "transaction_id",
    "date",
    "gender",
    "age",
    "age_group",
    "product_category",
    "quantity",
    "price_per_unit",
    "total_amount"
]

df = df.select(*required_columns)

# Keep one source row per business key before merge
df = df.dropDuplicates(["transaction_id"])

# Add audit columns
df = df.withColumn(
    "ingestion_timestamp",
    current_timestamp()
)

df = df.withColumn(
    "source_file",
    input_file_name()
)

# Initial Load
if not os.path.exists(bronze_path):

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(bronze_path)
    )

    print(f"Initial Bronze Load Completed from {source_file_path}!")

# Incremental MERGE
else:

    # Load existing Delta table
    delta_table = DeltaTable.forPath(
        spark,
        bronze_path
    )

    # MERGE operation
    (
        delta_table.alias("target")
        .merge(
            df.alias("source"),
            "target.transaction_id = source.transaction_id"
        )
        .whenMatchedUpdate(
            set={
                "date": "source.date",
                "gender": "source.gender",
                "age": "source.age",
                "age_group": "source.age_group",
                "product_category": "source.product_category",
                "quantity": "source.quantity",
                "price_per_unit": "source.price_per_unit",
                "total_amount": "source.total_amount",
                "ingestion_timestamp": "source.ingestion_timestamp",
                "source_file": "source.source_file"
            }
        )
        .whenNotMatchedInsert(
            values={
                "transaction_id": "source.transaction_id",
                "date": "source.date",
                "gender": "source.gender",
                "age": "source.age",
                "age_group": "source.age_group",
                "product_category": "source.product_category",
                "quantity": "source.quantity",
                "price_per_unit": "source.price_per_unit",
                "total_amount": "source.total_amount",
                "ingestion_timestamp": "source.ingestion_timestamp",
                "source_file": "source.source_file"
            }
        )
        .execute()
    )

    print(f"Incremental MERGE Completed Successfully from {source_file_path}!")

# Read final Bronze table
final_df = spark.read.format("delta").load(bronze_path)

# Show final data
print("\nFinal Bronze Data:")
final_df.show(3, truncate=False, vertical=True)

spark.stop()
