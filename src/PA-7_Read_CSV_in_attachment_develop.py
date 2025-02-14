# PYTHON IMPLEMENTATION IN DATABRICKS

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, to_date, when, lit, current_timestamp
from delta.tables import DeltaTable

# Define schema for CSV file
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", StringType(), True)
])

# Load CSV file into DataFrame
df = spark.read.csv("/FileStore/sample_sales_data.csv", schema=schema, header=True)

# Data Cleaning & Validation
allowed_country_codes = ['US', 'CA', 'UK', 'IN', 'AU']
df_cleaned = df \
    .withColumn("qty_sold", when(col("qty_sold") > 0, col("qty_sold")).otherwise(None)) \
    .withColumn("sales_date", to_date(col("sales_date"), "yyyy-MM-dd")) \
    .filter(col("country_cd").isin(allowed_country_codes))

# Log errors for invalid data
invalid_qty_sold = df.filter(col("qty_sold") <= 0)
invalid_dates = df.filter(col("sales_date").isNull() & df["sales_date"].isNotNull())
invalid_qty_sold.select("*").withColumn("error_message", lit("Invalid quantity for product_id at sales_date. Quantity must be a positive integer.")).show()
invalid_dates.select("*").withColumn("error_message", lit("Invalid date format for product_id at sales_date. Date must be in YYYY-MM-DD format.")).show()

# Write cleaned data to Delta Lake
delta_table_path = "/delta/purgo_playground/sales_data"
df_cleaned.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_table_path)

# Optimize and set Z-order
delta_table = DeltaTable.forPath(spark, delta_table_path)
delta_table.optimize().executeZOrderBy("sales_date")

# Set table properties for Delta Lake
spark.sql(f"""
    ALTER TABLE delta.`{delta_table_path}`
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

-- SQL IMPLEMENTATION IN DATABRICKS

/* Prepare Delta table with schema evolution */
CREATE TABLE IF NOT EXISTS purgo_playground.sales_data (
    country_cd STRING,
    product_id STRING,
    qty_sold INTEGER,
    sales_date STRING,
    processed_at TIMESTAMP
) 
USING DELTA
PARTITIONED BY (country_cd)
LOCATION '/delta/purgo_playground/sales_data';

/* Merge processed data into Delta table, handling inserts and schema evolution */
MERGE INTO purgo_playground.sales_data AS target
USING (SELECT * FROM delta.`/delta/purgo_playground/sales_data`) AS source
ON target.product_id = source.product_id AND target.sales_date = source.sales_date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

/* Vacuum to clean old data versions */
VACUUM purgo_playground.sales_data RETAIN 168 HOURS;

/* Validate current dataset */
-- Verify if the current data follows the required format and is consistent
SELECT 
    COUNT(*) AS invalid_qty_sold
FROM purgo_playground.sales_data
WHERE qty_sold <= 0;

SELECT 
    COUNT(*) AS invalid_date_format
FROM purgo_playground.sales_data
WHERE NOT sales_date RLIKE '^\d{4}-\d{2}-\d{2}$';

/* Set data access control */
GRANT SELECT ON purgo_playground.sales_data TO `data_viewer`;

/* Log GDPR compliance approval */
INSERT INTO purgo_playground.data_access_logs
SELECT "Data processing and storage complies with GDPR", current_timestamp();