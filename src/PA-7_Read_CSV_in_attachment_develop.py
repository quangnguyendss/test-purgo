from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, expr, isnull, rlike, current_timestamp, date_trunc, when
from delta.tables import DeltaTable

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ProductionReadyDatabricksEnvironment") \
    .getOrCreate()

# Define schema for sample sales data
sales_schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", TimestampType(), True)
])

# Load sample data from CSV
file_path = "/mnt/data/sample_sales_data.csv"
sales_df = spark.read.format("csv") \
    .option("header", True) \
    .schema(sales_schema) \
    .load(file_path)

# Data validation logic
valid_sales_df = sales_df.filter(
    (col("country_cd") != "") &
    (col("product_id").rlike("^P[0-9]{4}$")) &
    (col("qty_sold") > 0) &
    (col("sales_date").isNotNull())
)

# Delta Lake integration
delta_path = "/mnt/delta/sales_data"
valid_sales_df.write.format("delta").mode("overwrite").save(delta_path)

# Delta MERGE operation as an example of data versioning
target_table = DeltaTable.forPath(spark, delta_path)
updates_df = sales_df.filter(col("qty_sold") <= 50)

(target_table.alias("tgt")
 .merge(updates_df.alias("src"), "tgt.product_id = src.product_id")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())

# Optimization strategies
# For partitioning strategy, assume 'country_cd' as partition column
valid_sales_df.write.format("delta") \
    .partitionBy("country_cd") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(delta_path)

# Z-Ordering on 'sales_date' to improve query performance
target_table.optimize().executeZOrderBy("sales_date")

# Vacuum Delta tables to remove old files
target_table.vacuum(168) # retainLast 168 hours (7 days)

# Error handling: Check for invalid data and log
invalid_sales_df = sales_df.exceptAll(valid_sales_df)

invalid_sales_df.write.format("json") \
    .mode("overwrite") \
    .save("/mnt/logs/invalid_sales_data.json")

# Caching strategies for frequently queried data
valid_sales_df.cache()

# End of the implementation

