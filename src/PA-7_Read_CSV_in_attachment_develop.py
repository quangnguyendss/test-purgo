# Python code using PySpark for data processing in Databricks

# Import necessary libraries
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Define schema according to provided specifications
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", DateType(), True)
])

# Load the CSV file into a DataFrame
df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("/path/to/sample_sales_data.csv")  # Provide the correct path

# Data Validation - Check for invalid 'qty_sold' format
invalid_qty_sold = df.filter(~df["qty_sold"].cast("integer").isNotNull())
if invalid_qty_sold.count() > 0:
    raise ValueError("Invalid data format for 'qty_sold'. Expected INTEGER.")

# Data Validation - Check for future 'sales_date'
future_sales_dates = df.filter(df["sales_date"] > F.current_date())
if future_sales_dates.count() > 0:
    raise ValueError("Sales date cannot be in the future.")

# Handling NULL values
df = df.fillna({'country_cd': 'Unknown', 'product_id': 'Unknown', 'qty_sold': 0, 'sales_date': '1970-01-01'})

# Transformation - Example of required transformations
df_transformed = df.withColumn("sales_date_transformed", F.date_format(df["sales_date"], "yyyyMMdd"))

# Write data to Delta Lake table
df_transformed.write.format("delta").mode("overwrite").partitionBy("country_cd").saveAsTable("purgo_playground.sales_data_delta")

# Example of MERGE operation
delta_table = DeltaTable.forName(spark, "purgo_playground.sales_data_delta")

delta_table.alias("target").merge(
    source=df_transformed.alias("source"),
    condition="target.product_id = source.product_id"
).whenMatchedUpdate(set={"qty_sold": "source.qty_sold"}) \
 .whenNotMatchedInsert(values={
     "country_cd": "source.country_cd",
     "product_id": "source.product_id",
     "qty_sold": "source.qty_sold",
     "sales_date": "source.sales_date",
     "sales_date_transformed": "source.sales_date_transformed"
 }).execute()

# Optimize and vacuum Delta table
spark.sql("OPTIMIZE purgo_playground.sales_data_delta ZORDER BY (product_id)")
spark.sql("VACUUM purgo_playground.sales_data_delta RETAIN 0 HOURS")

# Error Handling for database connectivity is inherently managed in try-except blocks
try:
    # Example operation: reading from the Delta table
    sales_data_df = spark.table("purgo_playground.sales_data_delta")
    sales_data_df.show()
except Exception as e:
    print(f"Failed to connect to database. Check your network connection. Error: {str(e)}")

-- SQL code for data processing in Databricks

/* Use the working schema */
USE purgo_playground;

/* Check and create Delta table if not exists */
IF NOT EXISTS(SELECT * FROM information_schema.tables WHERE table_name = 'sales_data_delta') THEN
  CREATE TABLE sales_data_delta (
    country_cd STRING,
    product_id STRING,
    qty_sold INTEGER,
    sales_date DATE,
    sales_date_transformed STRING
  ) USING DELTA
  PARTITIONED BY (country_cd)
  TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = true, 'delta.autoOptimize.autoCompact' = true);
END IF;

/* Data Quality Checks */
-- Check for invalid qty_sold entries
SELECT * FROM sales_data_delta WHERE qty_sold IS NULL OR qty_sold < 0;

-- Check for future sales dates
SELECT * FROM sales_data_delta WHERE sales_date > CURRENT_DATE;

/* Example of Window Function */
SELECT country_cd, product_id, MAX(qty_sold) OVER (PARTITION BY country_cd) as max_qty
FROM sales_data_delta;

/* Cluster and Z-Order tables for performance improvements */
OPTIMIZE sales_data_delta ZORDER BY (product_id);

/* Vacuum the table to remove old files */
VACUUM sales_data_delta RETAIN 0 HOURS;