# Import necessary libraries for PySpark testing
from pyspark.sql.functions import col, current_date, when, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.utils import AnalysisException

# Define schema according to Databricks native data types
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", DateType(), True)
])

# Load data from CSV, adjust the path to the attached CSV file accordingly
csv_file_path = "/FileStore/tables/sample_sales_data.csv"  # Update with actual path

df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load(csv_file_path)

# Validate 'qty_sold' data type
df_validated = df.withColumn("validation_check",
                             when(df["qty_sold"].cast(IntegerType()).isNull(), "Invalid data format for 'qty_sold'. Expected INTEGER.")
                             .otherwise(None))

# Filter out invalid entries and log or handle them appropriately
invalid_entries = df_validated.filter(col("validation_check").isNotNull())
valid_entries = df_validated.filter(col("validation_check").isNull())

# Handle future dates
valid_entries = valid_entries.withColumn("future_date_check",
                                         when(valid_entries["sales_date"] > current_date(), "Sales date cannot be in the future.")
                                         .otherwise(None))

# Separate problematic records
future_date_issues = valid_entries.filter(col("future_date_check").isNotNull())
valid_entries = valid_entries.filter(col("future_date_check").isNull())

# Logging or storing issues for reporting
invalid_entries.show()
future_date_issues.show()

# Define Delta Lake table properties and create or replace the table
table_path = "dbfs:/user/hive/warehouse/purgo_playground.db/sales_data_delta"
valid_entries.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("country_cd") \
    .save(table_path)

spark.sql(f"CREATE TABLE IF NOT EXISTS purgo_playground.sales_data_delta USING DELTA LOCATION '{table_path}'")

# Delta Lake operations such as VACUUM, OPTIMIZE, etc.
spark.sql("OPTIMIZE purgo_playground.sales_data_delta ZORDER BY (product_id)")
spark.sql("VACUUM purgo_playground.sales_data_delta RETAIN 0 HOURS")  # Adjust retention as per policy

# Capture improvements or further manipulation as needed
try:
    # Demonstrate Delta Lake's merge capabilities for incremental data loads
    merge_data = valid_entries.alias("source")
    target_table = "purgo_playground.sales_data_delta"
    
    # SQL statement for performing merge
    merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING (SELECT * FROM source) 
        ON target.product_id = source.product_id AND target.sales_date = source.sales_date
        WHEN MATCHED THEN UPDATE SET qty_sold = source.qty_sold
        WHEN NOT MATCHED THEN INSERT *
    """
    
    # Performing the merge operation
    spark.sql(merge_sql)
    
except AnalysisException as e:
    print(f"An error occurred: {e}")

-- SQL test and validation operations

-- Ensure Unity Catalog schema "purgo_playground" is created
CREATE SCHEMA IF NOT EXISTS purgo_playground;

-- Create Delta Lake table if not exists
CREATE TABLE IF NOT EXISTS purgo_playground.sales_data_delta (
    country_cd STRING,
    product_id STRING,
    qty_sold INTEGER,
    sales_date DATE
) USING DELTA;

-- Query to validate correct ingestion and data storage
SELECT country_cd, product_id, qty_sold, CAST(sales_date AS STRING) as sales_date_str
FROM purgo_playground.sales_data_delta;

-- Ensure validations
-- Check for future sales_date entries
SELECT COUNT(*) FROM purgo_playground.sales_data_delta WHERE sales_date > CURRENT_DATE();

-- Handle dealing with previously invalid entries for logging or viewing
-- Example: SELECT the invalid entries captured in the ingestion process.
-- This would typically be handled in Python/PySpark data flow.

-- Handled future dates, non-numeric 'qty_sold', or any other violations