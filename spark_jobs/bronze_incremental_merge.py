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
bronze_path = "delta/bronze/retail_sales"

# Read CSV
df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/retail_sales.csv")
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

    print("Initial Bronze Load Completed!")

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
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Incremental MERGE Completed Successfully!")

# Read final Bronze table
final_df = spark.read.format("delta").load(bronze_path)

# Show final data
print("\nFinal Bronze Data:")
final_df.show(3, truncate=False, vertical=True)

spark.stop()