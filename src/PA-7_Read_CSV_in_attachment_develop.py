from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, sum as _sum
from delta.tables import DeltaTable

# Create a Spark session
spark = SparkSession.builder.appName("Sales Data Processing").getOrCreate()

# Define schema
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", DateType(), True)
])

# Load data
file_path = "/path/to/sample_sales_data.csv"
sales_df = spark.read.csv(file_path, schema=schema, header=True)

# Handle errors for incorrect formats
try:
    sales_df = sales_df.withColumn("sales_date", col("sales_date").cast(DateType()))
except Exception as e:
    print("Data Format Error: Invalid data format for sales_date", str(e))
    raise

# Filter out rows with null values in crucial columns
sales_df = sales_df.dropna(subset=["country_cd", "product_id", "qty_sold"])

# Write to Delta Lake
delta_table_path = f"/mnt/delta/purgo_playground/sales_data_delta"
sales_df.write.format("delta").mode("overwrite").save(delta_table_path)

# Verify by reading from Delta Lake
sales_delta_df = spark.read.format("delta").load(delta_table_path)
sales_delta_df.createOrReplaceTempView("sales_data_delta")

# Validate data integrity
product_sales_check = spark.sql("""
    SELECT product_id, SUM(qty_sold) AS total_qty_sold
    FROM sales_data_delta
    GROUP BY product_id
""")
expected_sales = [("P1001", 150), ("P1002", 103), ("P1003", 70), ("P1004", 75)]
expected_df = spark.createDataFrame(expected_sales, schema=["product_id", "total_qty_sold"])
assert product_sales_check.collect() == expected_df.collect(), "Data integrity check failed."

# Demonstrate Delta Lake time travel (reading history)
historical_sales_df = spark.read.format("delta").option("versionAsOf", 0).load(delta_table_path)

# Perform Delta Lake merge operation
merge_data = [("US", "P1001", 70, "2024-01-15")]
merge_df = spark.createDataFrame(merge_data, schema=schema)
merge_df.createOrReplaceTempView("merge_data")

DeltaTable.forPath(spark, delta_table_path).alias("target").merge(
    merge_df.alias("source"),
    "target.country_cd = source.country_cd AND target.product_id = source.product_id"
).whenMatchedUpdate(set={"qty_sold": "source.qty_sold"}).whenNotMatchedInsertAll().execute()

# Optimize and vacuum the Delta table
spark.sql(f"OPTIMIZE '{delta_table_path}' ZORDER BY (country_cd, product_id)")
spark.sql(f"VACUUM '{delta_table_path}' RETAIN 168 HOURS")

# Performance optimization: Cache the DataFrame
sales_delta_df.cache()

# Stop Spark session
spark.stop()
