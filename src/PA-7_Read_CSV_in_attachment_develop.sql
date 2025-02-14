-- SQL Code for Databricks Environment

-- Create Delta Table for sales data in Unity Catalog schema purgo_playground
CREATE TABLE IF NOT EXISTS purgo_playground.sales_data (
    country_cd STRING,
    product_id STRING,
    qty_sold INT,
    sales_date DATE
)
USING DELTA
PARTITIONED BY (country_cd)
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

-- Load CSV data into a temporary view
CREATE OR REPLACE TEMPORARY VIEW sales_data_temp AS
SELECT *
FROM csv.`/FileStore/tables/sample_sales_data.csv`;

-- Insert data from temporary view to Delta table
MERGE INTO purgo_playground.sales_data AS target
USING (
    SELECT country_cd, product_id, CAST(qty_sold AS INT) AS qty_sold, TO_DATE(CAST(UNIX_TIMESTAMP(sales_date, 'yyyy-MM-dd') AS TIMESTAMP)) AS sales_date
    FROM sales_data_temp
    WHERE qty_sold RLIKE '^[0-9]+$' AND sales_date <= CURRENT_DATE()
) AS source
ON target.country_cd = source.country_cd AND target.product_id = source.product_id
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *;

-- Optimize and Z-Order the table for performance
OPTIMIZE purgo_playground.sales_data
ZORDER BY (sales_date, product_id);

-- Vacuum out old versions
VACUUM purgo_playground.sales_data RETAIN 0 HOURS;

-- Data quality checks
-- Check for invalid qty_sold and future sales_date records
CREATE OR REPLACE TEMPORARY VIEW sales_data_checks AS
SELECT 
    country_cd,
    product_id,
    qty_sold,
    sales_date,
    CASE WHEN NOT qty_sold RLIKE '^[0-9]+$' THEN 'Invalid qty_sold format' ELSE NULL END AS error_qty,
    CASE WHEN sales_date > CURRENT_DATE() THEN 'Future sales_date' ELSE NULL END AS error_date
FROM sales_data_temp;

-- Fetch any rows with data issues
SELECT *
FROM sales_data_checks
WHERE error_qty IS NOT NULL OR error_date IS NOT NULL;

-- Drop temporary view after processing
DROP VIEW IF EXISTS sales_data_temp;
DROP VIEW IF EXISTS sales_data_checks;

# PySpark Code for Databricks Environment

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Define schema for the csv data
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", StringType(), True)
])

# Load csv data into DataFrame
file_path = "/FileStore/tables/sample_sales_data.csv"
df_sales_data = spark.read.csv(file_path, header=True, schema=schema)

# Data transformations: Cast sales_date to DateType and Filter for valid records
df_sales_valid = df_sales_data \
    .withColumn("sales_date", F.to_date(df_sales_data.sales_date, 'yyyy-MM-dd')) \
    .filter(F.col("qty_sold").cast("Int").isNotNull() & (F.col("sales_date") <= F.current_date()))

# Create Delta Table if not exists and write data
table_path = "dbfs:/user/hive/warehouse/purgo_playground.db/sales_data"
if not DeltaTable.isDeltaTable(spark, table_path):
    df_sales_valid.write.format("delta").partitionBy("country_cd").option("overwriteSchema", "true").saveAsTable("purgo_playground.sales_data")

# Upsert data into Delta Table
delta_table = DeltaTable.forName(spark, "purgo_playground.sales_data")
delta_table.alias("target").merge(
    df_sales_valid.alias("source"),
    "target.country_cd = source.country_cd AND target.product_id = source.product_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Perform optimization and Z-ordering for performance
spark.sql("OPTIMIZE purgo_playground.sales_data ZORDER BY (sales_date, product_id)")

# Perform vacuuming to clean up old versions
spark.sql("VACUUM purgo_playground.sales_data RETAIN 0 HOURS")