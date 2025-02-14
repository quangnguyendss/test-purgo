-- Setup for validating and loading sales data with quality checks
-- Assuming the necessary tables in the Unity Catalog and schema setup

-- Load sales data into a temporary staging table
CREATE OR REPLACE TEMP VIEW sales_staging AS
SELECT 
    country_cd, 
    qty_sold, 
    product_id, 
    sale_date
FROM csv.`dbfs:/FileStore/tables/sales_20240611.csv`
USING OPTIONS (header = 'true', inferSchema = 'true');

-- Validate the data and process accordingly

-- Check for NULL country_cd
SELECT COUNT(*) AS null_country_cd_count FROM sales_staging WHERE country_cd IS NULL;

-- Check for non-numeric qty_sold
SELECT COUNT(*) AS non_numeric_qty_sold_count FROM sales_staging WHERE NOT qty_sold RLIKE '^[0-9]+$';

-- Check for duplicate product_id
SELECT COUNT(*) AS duplicate_product_id_count FROM (
  SELECT product_id, COUNT(*) AS cnt 
  FROM sales_staging 
  GROUP BY product_id 
  HAVING cnt > 1
);

-- Check for correct sale_date format (yyyy-mm-dd)
SELECT COUNT(*) AS incorrect_date_format_count FROM sales_staging WHERE NOT sale_date RLIKE '^(19|20)\d\d-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$';

-- Insert valid data into the sales table in the Unity Catalog
INSERT INTO purgo_playground.sales 
SELECT 
    country_cd, 
    CAST(qty_sold AS INTEGER), 
    product_id, 
    TO_DATE(sale_date, 'yyyy-mm-dd') 
FROM sales_staging
WHERE 
    country_cd IS NOT NULL 
    AND qty_sold RLIKE '^[0-9]+$' 
    AND product_id NOT IN (
      SELECT product_id 
      FROM sales_staging 
      GROUP BY product_id 
      HAVING COUNT(*) > 1
    )
    AND sale_date RLIKE '^(19|20)\d\d-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$';

-- Insert error records into the exception table for review
INSERT INTO purgo_playground.exception
SELECT 
    country_cd, 
    qty_sold, 
    product_id, 
    sale_date
FROM sales_staging
WHERE 
    country_cd IS NULL 
    OR NOT qty_sold RLIKE '^[0-9]+$' 
    OR product_id IN (
      SELECT product_id 
      FROM sales_staging 
      GROUP BY product_id 
      HAVING COUNT(*) > 1
    )
    OR NOT sale_date RLIKE '^(19|20)\d\d-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$';

from pyspark.sql.functions import col, row_number, to_date
from pyspark.sql.window import Window

# Load CSV into DataFrame with predefined schema
df_sales = spark.read.format("csv") \
    .option("header", "true") \
    .load("dbfs:/FileStore/tables/sales_20240611.csv")

# Validate non-null country_cd
df_null_country_cd = df_sales.filter(col("country_cd").isNull())
df_null_country_cd.createOrReplaceTempView("null_country_cd")

# Validate numeric qty_sold
df_non_numeric_qty_sold = df_sales.filter(~col("qty_sold").rlike("^[0-9]+$"))
df_non_numeric_qty_sold.createOrReplaceTempView("non_numeric_qty_sold")

# Validate unique product_id with window function
window_spec = Window.partitionBy("product_id")
df_product_id_duplicates = df_sales.withColumn("row_number", row_number().over(window_spec)) \
    .filter(col("row_number") > 1)
df_product_id_duplicates.createOrReplaceTempView("duplicate_product_ids")

# Validate sale_date format
df_invalid_date_format = df_sales.filter(~col("sale_date").rlike("^(19|20)\\d\\d-(0[1-9]|1[012])-(0[0-9]|[12][0-9]|3[01])$"))
df_invalid_date_format.createOrReplaceTempView("invalid_date_format")

# Filtering valid records for insertion into the sales table
df_valid_sales = df_sales.filter(
    col("country_cd").isNotNull() &
    col("qty_sold").rlike("^[0-9]+$") &
    ~col("product_id").isin([row.product_id for row in df_product_id_duplicates.collect()]) &
    col("sale_date").rlike("^(19|20)\\d\\d-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$")
)

# Save valid records to sales table
df_valid_sales.select(
    col("country_cd"),
    col("qty_sold").cast("integer"),
    col("product_id"),
    to_date(col("sale_date"), "yyyy-MM-dd").alias("sale_date")
).write.option("mergeSchema", "true").format("delta").mode("append").saveAsTable("purgo_playground.sales")

# Combine error records for insertion into exception handler table
df_invalid_records = df_null_country_cd.union(df_non_numeric_qty_sold).union(df_product_id_duplicates).union(df_invalid_date_format)

# Save invalid records to exception table
df_invalid_records.write.option("mergeSchema", "true").format("delta").mode("append").saveAsTable("purgo_playground.exception")