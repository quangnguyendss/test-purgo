-- SQL Code begins here

/* Install necessary libraries for SQL testing if any specific library needs to be installed.
There is no explicit library installation needed for SQL as we're using Databricks SQL syntax */

/* Validate the Unity Catalog Schema and table existence */
USE purgo_playground;
IF NOT EXISTS(SELECT * FROM information_schema.tables WHERE table_name = 'sales_data') THEN
  -- Create table procedure if not exists
  CREATE TABLE sales_data (
    country_cd STRING,
    product_id STRING,
    qty_sold INTEGER,
    sales_date DATE
  );
END IF;

/* Validate DataType Test for the 'country_cd' column */
ALTER TABLE sales_data ADD COLUMNS (tmp_country_cd STRING);
INSERT INTO sales_data(tmp_country_cd) VALUES ('US');
UPDATE sales_data SET country_cd = CAST(tmp_country_cd AS STRING);

/* Validate DataType Test for the 'qty_sold' column */
ALTER TABLE sales_data ADD COLUMNS (tmp_qty_sold STRING);
INSERT INTO sales_data(tmp_qty_sold) VALUES ('100');
UPDATE sales_data SET qty_sold = CAST(tmp_qty_sold AS INTEGER);

/* Validate NULL handling for each column */
ALTER TABLE sales_data ADD COLUMNS (tmp_country STRING);
INSERT INTO sales_data(tmp_country) VALUES (NULL);
UPDATE sales_data SET country_cd = tmp_country WHERE tmp_country IS NULL;
-- Repeat similar blocks for product_id, qty_sold, sales_date */

/* Test Delta Lake Operations: MERGE, UPDATE, DELETE */
-- MERGE Example
MERGE INTO sales_data AS target
USING (SELECT 'US' as country_cd, 'P1005' as product_id, 10 as qty_sold, CURRENT_DATE as sales_date) AS src
ON target.product_id = src.product_id
WHEN MATCHED THEN
  UPDATE SET target.qty_sold = src.qty_sold
WHEN NOT MATCHED THEN
  INSERT (country_cd, product_id, qty_sold, sales_date)
  VALUES (src.country_cd, src.product_id, src.qty_sold, src.sales_date);

-- Window Function Test
SELECT country_cd, product_id, MAX(qty_sold) OVER (PARTITION BY country_cd) as max_qty
FROM sales_data;

/* Ensure proper cleanup operations */
TRUNCATE TABLE sales_data;

-- SQL Code ends here

# Python code using PySpark for tests

# Import the necessary libraries
# Ensure Spark session is initialized
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql import functions as F

# Setup Spark session
spark = SparkSession.builder \
    .appName('Databricks SQL Testing') \
    .getOrCreate()

# Define schema according to test case requirements
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", DateType(), True)
])

# Sample data for testing purpose
data = [("US", "P1001", 50, "2024-01-15"),
        ("US", "P1002", 30, "2024-01-16"),
        ("CA", "P1003", 25, "2024-01-17"),
        ("IN", "P1001", 60, "2024-01-20"),
        ("AU", "P1004", 55, "2024-01-22")]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Perform assertions using PySpark built-in functions
assert df.filter(df['qty_sold'] < 0).count() == 0, "Negative quantity sold exists"
assert df.filter(df['sales_date'] > F.current_date()).count() == 0, "Future sales date exists"

# Convert Data Types as needed
df = df.withColumn("qty_sold", df["qty_sold"].cast("int"))
df = df.withColumn("sales_date", F.to_date(df["sales_date"], "yyyy-MM-dd"))

# Handle NULL values
df = df.fillna({'country_cd': 'Unknown', 'product_id': 'Unknown', 'qty_sold': 0, 'sales_date': '1970-01-01'})

# Test Delta operations
# Assuming Delta Lake is used, validate merging process
from delta.tables import *

# Create Delta table for testing
df.write.format("delta").mode("overwrite").save("/tmp/sales_data_delta")

# Read Delta table
delta_table = DeltaTable.forPath(spark, "/tmp/sales_data_delta")

# Perform Delta MERGE operation for testing
delta_table.alias("target").merge(
    source=df.alias("source"),
    condition="target.product_id = source.product_id") \
  .whenMatchedUpdate(set={"qty_sold": "source.qty_sold"}) \
  .whenNotMatchedInsert(values={"country_cd": "source.country_cd", "product_id": "source.product_id",
                                "qty_sold": "source.qty_sold", "sales_date": "source.sales_date"}) \
  .execute()

# Validate the MERGE operation
assert delta_table.toDF().filter(delta_table.toDF()['product_id'] == 'P1001').count() == 1, "Merge operation failed"

# Cleanup operations
delta_table.delete("true")