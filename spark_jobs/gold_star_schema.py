
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os
import warnings
warnings.filterwarnings("ignore")
# =====================================================
# CREATE SPARK SESSION
# =====================================================

builder = (
    SparkSession.builder
    .appName("Gold Star Schema")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# =====================================================
# READ SILVER LAYER
# =====================================================

delta_base_path = os.getenv("DELTA_BASE_PATH", "delta")
silver_path = os.path.join(delta_base_path, "silver", "retail_sales")
dim_customer_path = os.path.join(delta_base_path, "gold", "dim_customer")
dim_product_path = os.path.join(delta_base_path, "gold", "dim_product")
dim_date_path = os.path.join(delta_base_path, "gold", "dim_date")
fact_sales_path = os.path.join(delta_base_path, "gold", "fact_sales")

silver_df = spark.read.format("delta").load(silver_path)

# Create Temp View
silver_df.createOrReplaceTempView("silver_retail")

# =====================================================
# DIM CUSTOMER
# =====================================================

dim_customer = spark.sql("""

WITH customer_cte AS (

    SELECT DISTINCT
        gender,
        age,
        age_group
    FROM silver_retail

)

SELECT
    ROW_NUMBER() OVER(ORDER BY gender, age_group, age) AS customer_key,
    gender,
    age,
    age_group

FROM customer_cte

""")

# =====================================================
# DIM PRODUCT
# =====================================================

dim_product = spark.sql("""

WITH product_cte AS (

    SELECT DISTINCT
        product_category,
        price_category
    FROM silver_retail

)

SELECT
    ROW_NUMBER() OVER(ORDER BY product_category) AS product_key,
    product_category,
    price_category

FROM product_cte

""")

# =====================================================
# DIM DATE
# =====================================================

dim_date = spark.sql("""

WITH date_cte AS (

    SELECT DISTINCT
        date
    FROM silver_retail

)

SELECT
    ROW_NUMBER() OVER(ORDER BY date) AS date_key,
    date,
    YEAR(date) AS year,
    MONTH(date) AS month,
    DAY(date) AS day,
    QUARTER(date) AS quarter

FROM date_cte

""")

# =====================================================
# CREATE TEMP VIEWS
# =====================================================

dim_customer.createOrReplaceTempView("dim_customer")

dim_product.createOrReplaceTempView("dim_product")

dim_date.createOrReplaceTempView("dim_date")

# =====================================================
# FACT SALES
# =====================================================

fact_sales = spark.sql("""

WITH fact_cte AS (

    SELECT
        s.transaction_id,
        c.customer_key,
        p.product_key,
        d.date_key,
        s.quantity,
        s.price_per_unit,
        s.total_amount

    FROM silver_retail s

    LEFT JOIN dim_customer c
        ON s.gender = c.gender
        AND s.age = c.age
        AND s.age_group = c.age_group

    LEFT JOIN dim_product p
        ON s.product_category = p.product_category
        AND s.price_category = p.price_category

    LEFT JOIN dim_date d
        ON s.date = d.date

)

SELECT *
FROM fact_cte

""")

# =====================================================
# WRITE GOLD TABLES
# =====================================================

(
    dim_customer.write
    .format("delta")
    .mode("overwrite")
    .save(dim_customer_path)
)

(
    dim_product.write
    .format("delta")
    .mode("overwrite")
    .save(dim_product_path)
)

(
    dim_date.write
    .format("delta")
    .mode("overwrite")
    .save(dim_date_path)
)

(
    fact_sales.write
    .format("delta")
    .mode("overwrite")
    .save(fact_sales_path)
)

# =====================================================
# SHOW OUTPUTS
# =====================================================

print("\nDIM CUSTOMER")
dim_customer.show(3,truncate=False)

print("\nDIM PRODUCT")
dim_product.show(3,truncate=False)

print("\nDIM DATE")
dim_date.show(3,truncate=False)

print("\nFACT SALES")
fact_sales.show(3,truncate=False)

print("\nGold Star Schema Created Successfully!")

spark.stop()
