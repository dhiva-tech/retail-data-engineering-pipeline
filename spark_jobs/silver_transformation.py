from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    upper,
    trim,
    when,
    current_date,
    datediff,
    coalesce,
    to_timestamp,
    try_to_timestamp
)
from delta import configure_spark_with_delta_pip
import warnings
warnings.filterwarnings("ignore")

# Create Spark Session
builder = (
    SparkSession.builder
    .appName("Silver Layer Transformation")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read Bronze Delta Table
df = spark.read.format("delta").load("delta/bronze/retail_sales")

# Convert date column to proper datatype

df = df.withColumn(
    "date",
    when(
        col("date").contains("-"),
        to_date(col("date"), "dd-MM-yyyy")
    ).otherwise(
        to_date(col("date"), "M/d/yyyy")
    )
)
# Convert numeric columns
numeric_columns = [
    "transaction_id",
    "age",
    "quantity",
    "price_per_unit",
    "total_amount"
]

for column in numeric_columns:
    df = df.withColumn(
        column,
        col(column).cast("int")
    )

# Standardize text columns
df = (
    df.withColumn("gender", upper(trim(col("gender"))))
      .withColumn("product_category", upper(trim(col("product_category"))))
      .withColumn("age_group", upper(trim(col("age_group"))))
)

# Handle null values
df = (
    df.fillna({
        "quantity": 0,
        "price_per_unit": 0,
        "total_amount": 0
    })
)

# Remove duplicate records
df = df.dropDuplicates()

# Filter invalid records
df = df.filter(
    (col("age") > 0) &
    (col("quantity") > 0) &
    (col("price_per_unit") > 0)
)

# Add derived column: price category
df = df.withColumn(
    "price_category",
    when(col("price_per_unit") >= 500, "HIGH")
    .when(col("price_per_unit") >= 100, "MEDIUM")
    .otherwise("LOW")
)

# Add derived column: total purchase value check
df = df.withColumn(
    "purchase_type",
    when(col("total_amount") >= 1000, "BULK_PURCHASE")
    .otherwise("REGULAR_PURCHASE")
)

# Add processing date
df = df.withColumn(
    "processing_date",
    current_date()
)

# Add days_since_purchase
df = df.withColumn(
    "days_since_purchase",
    datediff(current_date(), col("date"))
)

# Show schema
print("\nSilver Schema:")
df.printSchema()

# Show transformed data
print("\nSilver Data:")
df.show(3, truncate=False, vertical=True)

# Write Silver Delta Table
(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save("delta/silver/retail_sales")
)

print("Silver layer created successfully!")

spark.stop()