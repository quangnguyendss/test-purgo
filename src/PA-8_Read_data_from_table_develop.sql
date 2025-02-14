-- SQL Code for Data Extraction and Validation

/* 
  Section: Data Extraction
  Extract valid records from the sample_users_data table
*/

SELECT country_cd, product_id, qty_sold, sales_date, valid
FROM agilisium_playground.purgo_playground.sample_users_data
WHERE valid = 1;

/* 
  Section: Data Validation
  Validate the format and integrity of the extracted data
*/

-- Validate country_cd format
SELECT * FROM agilisium_playground.purgo_playground.sample_users_data
WHERE valid = 1
AND country_cd NOT REGEXP '^[A-Z]{2}$';

-- Validate product_id format
SELECT * FROM agilisium_playground.purgo_playground.sample_users_data
WHERE valid = 1
AND product_id NOT REGEXP '^P\d{4}$';

-- Validate qty_sold is positive
SELECT * FROM agilisium_playground.purgo_playground.sample_users_data
WHERE valid = 1
AND qty_sold < 0;

-- Validate sales_date format
SELECT * FROM agilisium_playground.purgo_playground.sample_users_data
WHERE valid = 1
AND sales_date NOT REGEXP '^\d{4}-\d{2}-\d{2}$';

# PySpark Code for Data Extraction and Validation

# Import necessary libraries
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col

# Define schema for sample_users_data
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", DateType(), True),
    StructField("valid", IntegerType(), True)
])

# Load data from the Delta table
df = spark.read.format("delta").schema(schema).load("/mnt/delta/agilisium_playground/purgo_playground/sample_users_data")

# Filter for valid records
valid_df = df.filter(col("valid") == 1)

# Data Quality Test: Validate country_cd format
invalid_country_cd_df = valid_df.filter(~col("country_cd").rlike("^[A-Z]{2}$"))
if invalid_country_cd_df.count() > 0:
    raise ValueError("Invalid country code format detected")

# Data Quality Test: Validate product_id format
invalid_product_id_df = valid_df.filter(~col("product_id").rlike("^P\d{4}$"))
if invalid_product_id_df.count() > 0:
    raise ValueError("Invalid product ID format detected")

# Data Quality Test: Validate qty_sold is positive
negative_qty_sold_df = valid_df.filter(col("qty_sold") < 0)
if negative_qty_sold_df.count() > 0:
    raise ValueError("Quantity sold cannot be negative")

# Data Quality Test: Validate sales_date format
invalid_sales_date_df = valid_df.filter(~col("sales_date").cast("string").rlike("^\d{4}-\d{2}-\d{2}$"))
if invalid_sales_date_df.count() > 0:
    raise ValueError("Invalid sales date format detected")