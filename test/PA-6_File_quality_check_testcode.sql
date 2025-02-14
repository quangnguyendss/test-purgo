-- Testing Databricks SQL functionalities and integration.

-- Validation and Cleanup process for sales data
-- Load sales data and apply quality checks before transferring to the target table.
-- Assumes the table scheme exists in purgo_playground

-- Load data into a staging table
-- This staging table helps in isolating raw data from production datasets
CREATE OR REPLACE TEMP VIEW sales_staging AS
SELECT * FROM (
  SELECT 
    CAST(NULL AS STRING) AS country_cd,      -- Placeholder for schema
    CAST(NULL AS STRING) AS qty_sold,        -- Placeholder for schema
    CAST(NULL AS STRING) AS product_id,      -- Placeholder for schema
    CAST(NULL AS STRING) AS sale_date        -- Placeholder for schema
) LIMIT 0;

-- Load data from the source CSV
SELECT * FROM csv.`dbfs:/FileStore/tables/sales_20240611.csv`
  USING OPTIONS (header = 'true')
  INTO sales_staging;

-- Apply validation checks

-- Check for NULL country_cd
SELECT COUNT(*) FROM sales_staging WHERE country_cd IS NULL;

-- Check for non-numeric qty_sold
SELECT COUNT(*) FROM sales_staging WHERE NOT qty_sold RLIKE '^[0-9]+$';

-- Check for duplicate product_id
SELECT COUNT(*) FROM (
  SELECT product_id, COUNT(*) AS cnt 
  FROM sales_staging 
  GROUP BY product_id 
  HAVING cnt > 1
);

-- Check for correct sale_date format (yyyy-mm-dd)
SELECT COUNT(*) FROM sales_staging WHERE NOT sale_date RLIKE '^(19|20)\d\d-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$';

-- Insert valid records into the target sales table
INSERT INTO purgo_playground.sales 
SELECT country_cd, CAST(qty_sold AS INTEGER), product_id, TO_DATE(sale_date, 'yyyy-mm-dd') 
FROM sales_staging
WHERE country_cd IS NOT NULL 
  AND qty_sold RLIKE '^[0-9]+$' 
  AND product_id NOT IN (
    SELECT product_id 
    FROM sales_staging 
    GROUP BY product_id 
    HAVING COUNT(*) > 1
  )
  AND sale_date RLIKE '^(19|20)\d\d-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$';

-- Insert error records into the exception table
INSERT INTO purgo_playground.exception
SELECT country_cd, qty_sold, product_id, sale_date
FROM sales_staging
WHERE country_cd IS NULL 
  OR NOT qty_sold RLIKE '^[0-9]+$' 
  OR product_id IN (
    SELECT product_id 
    FROM sales_staging 
    GROUP BY product_id 
    HAVING COUNT(*) > 1
  )
  OR NOT sale_date RLIKE '^(19|20)\d\d-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$';

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, row_number, isnull
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Databricks SQL and PySpark Testing") \
    .getOrCreate()

# Load CSV into DataFrame with predefined schema
source_path = "dbfs:/FileStore/tables/sales_20240611.csv"
df_sales = spark.read.format("csv") \
    .option("header", "true") \
    .load(source_path)

# Validate country_cd not null
null_country_cd_count = df_sales.filter(col("country_cd").isNull()).count()
assert null_country_cd_count == 0, f"country_cd column has null values: {null_country_cd_count}"

# Validate qty_sold is numeric
non_numeric_qty_sold_count = df_sales.filter(~col("qty_sold").rlike("^[0-9]+$")).count()
assert non_numeric_qty_sold_count == 0, f"qty_sold column has non-numeric values: {non_numeric_qty_sold_count}"

# Validate unique product_id
window_spec = Window.partitionBy("product_id")
product_id_duplicate_count = df_sales.withColumn("row_number", row_number().over(window_spec)) \
    .filter(col("row_number") > 1).count()
assert product_id_duplicate_count == 0, f"product_id column has duplicates: {product_id_duplicate_count}"

# Validate sale_date format
invalid_date_format_count = df_sales.filter(~col("sale_date").rlike("^(19|20)\\d\\d-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$")).count()
assert invalid_date_format_count == 0, f"sale_date column has wrong format: {invalid_date_format_count}"

# Data cleaning and insertion into sales table
valid_records = df_sales.filter(
    col("country_cd").isNotNull() &
    col("qty_sold").rlike("^[0-9]+$") &
    col("sale_date").rlike("^(19|20)\\d\\d-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$") &
    ~df_sales.product_id.isin([row['product_id'] for row in df_sales.groupBy("product_id").count().filter("count > 1").collect()])
)
valid_records.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("purgo_playground.sales")

# Insert invalid records into the exception table
invalid_records = df_sales.filter(
    col("country_cd").isNull() |
    ~col("qty_sold").rlike("^[0-9]+$") |
    col("sale_date").isNull() |
    col("sale_date").isNotNull() & ~col("sale_date").rlike("^(19|20)\\d\\d-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$") |
    df_sales.product_id.isin([row['product_id'] for row in df_sales.groupBy("product_id").count().filter("count > 1").collect()])
)
invalid_records.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("purgo_playground.exception")