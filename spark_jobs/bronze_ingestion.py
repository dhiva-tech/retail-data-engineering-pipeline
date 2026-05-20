from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from delta import configure_spark_with_delta_pip
import warnings
warnings.filterwarnings("ignore")

# Create Spark Session with Delta support
builder = (
    SparkSession.builder
    .appName("Bronze Layer Ingestion")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read CSV file
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

# Select only required columns
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

# Add ingestion timestamp
df = df.withColumn(
    "ingestion_timestamp",
    current_timestamp()
)

# Add source file name
df = df.withColumn(
    "source_file",
    input_file_name()
)

# Show schema
print("\nBronze Schema:")
df.printSchema()

# Show sample data
print("\nBronze Data:")
df.show(3, truncate=False, vertical=True)

# Write Bronze Delta Table
(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save("delta/bronze/retail_sales")
)

print("Bronze layer created successfully!")

spark.stop()