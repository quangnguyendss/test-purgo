-- Setup Delta Lake environment with necessary configurations

-- Create Delta Table with appropriate schema
CREATE TABLE IF NOT EXISTS purgo_playground.sales_data (
  country_cd STRING,
  product_id STRING,
  qty_sold INT,
  sales_date TIMESTAMP
)
USING DELTA
PARTITIONED BY (country_cd)
COMMENT 'Table for storing sales data'
LOCATION '/delta/purgo_playground/sales_data';

-- Load data from CSV into Delta Table
COPY INTO purgo_playground.sales_data
FROM '/FileStore/tables/sample_sales_data.csv'
FILEFORMAT = 'CSV'
FORMAT_OPTIONS ('header' = 'true');

/* Implement data validation before processing */
/* Validate country code format */
SELECT country_cd
FROM purgo_playground.sales_data
WHERE LENGTH(country_cd) != 2 OR country_cd NOT RLIKE '^[A-Z]{2}$'
LIMIT 10;

/* Validate non-negative qty_sold */
SELECT *
FROM purgo_playground.sales_data
WHERE qty_sold < 0
LIMIT 10;

/* Validate product ID pattern */
SELECT *
FROM purgo_playground.sales_data
WHERE product_id NOT LIKE 'P____'
LIMIT 10;

/* Validate sales_date format */
SELECT sales_date
FROM purgo_playground.sales_data
WHERE sales_date IS NULL OR sales_date NOT RLIKE '^\d{4}-\d{2}-\d{2}$'
LIMIT 10;

/* Handle duplicate entries */
DELETE FROM purgo_playground.sales_data AS a
USING (
  SELECT country_cd, product_id, sales_date, COUNT(*) as cnt
  FROM purgo_playground.sales_data
  GROUP BY country_cd, product_id, sales_date
  HAVING cnt > 1
) AS b
WHERE a.country_cd = b.country_cd AND a.product_id = b.product_id AND a.sales_date = b.sales_date;

/* Optimize table using Z-Ordering */
OPTIMIZE purgo_playground.sales_data
ZORDER BY (product_id);

-- Schedule periodic table vacuuming
SET spark.databricks.delta.retentionDurationCheck.enabled = false;

VACUUM purgo_playground.sales_data RETAIN 168 HOURS;

# PySpark Implementation for Data Processing

from pyspark.sql.functions import col, expr

# Define input file path
file_path = "/FileStore/tables/sample_sales_data.csv"

# Load data into DataFrame
sales_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Data validation and processing
sales_df = sales_df.withColumn("country_cd", expr("UPPER(TRIM(country_cd))")) \
                    .filter(col("country_cd").rlike("^[A-Z]{2}$")) \
                    .filter(col("product_id").rlike("^P\\d{4}$")) \
                    .filter(col("qty_sold").cast("int").isNotNull() & (col("qty_sold") >= 0)) \
                    .withColumn("sales_date", expr("TO_DATE(sales_date, 'yyyy-MM-dd')"))

# Handle duplicate entries
window_spec = Window.partitionBy("country_cd", "product_id", "sales_date").orderBy("sales_date")
deduped_sales_df = sales_df.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1).drop("row_num")

# Write DataFrame to Delta Lake
deduped_sales_df.write.format("delta").mode("overwrite").partitionBy("country_cd").save("/delta/purgo_playground/sales_data")

# Cache the DataFrame for further operations
deduped_sales_df.cache()

# Utilize Delta Lake Merge for data update
new_sales_df = spark.read.csv("/FileStore/tables/new_sales_data.csv", header=True, inferSchema=True)
new_sales_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.new_sales_data")

spark.sql("""
MERGE INTO purgo_playground.sales_data AS target
USING purgo_playground.new_sales_data AS source
ON target.product_id = source.product_id AND target.sales_date = source.sales_date
WHEN MATCHED THEN
  UPDATE SET target.qty_sold = target.qty_sold + source.qty_sold
WHEN NOT MATCHED
  THEN INSERT (country_cd, product_id, qty_sold, sales_date)
  VALUES (source.country_cd, source.product_id, source.qty_sold, source.sales_date)
""")