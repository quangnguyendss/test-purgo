from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from delta.tables import DeltaTable
from pyspark.sql import functions as F
import os

# Initialize Spark session with Delta support
spark = SparkSession.builder \
    .appName("SalesDataProcessing") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

# Define schema for CSV
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", DateType(), True)
])

input_path = "dbfs:/mnt/<your_mount>/sample_sales_data.csv"
output_table = "purgo_playground.sales_data"

# Read CSV file with schema
df = spark.read.format("csv").option("header", "true").schema(schema).load(input_path)

# Convert sales_date to date format
df = df.withColumn("sales_date", F.to_date(df["sales_date"], "yyyy-MM-dd"))

# Write data to Delta
df.write.format("delta").mode("overwrite").saveAsTable(output_table)

# Implement Delta Lake operations
# Add a new record for validation
new_data = [("US", "P1001", 10, "2024-01-24")]
new_df = spark.createDataFrame(new_data, schema)

delta_table = DeltaTable.forName(spark, output_table)

# Merge operation for Delta Lake
delta_table.alias("target").merge(
    new_df.alias("source"),
    "target.country_cd = source.country_cd AND target.product_id = source.product_id"
).whenMatchedUpdate(
    set={"qty_sold": "target.qty_sold + source.qty_sold"}
).whenNotMatchedInsert(
    values={
        "country_cd": "source.country_cd",
        "product_id": "source.product_id",
        "qty_sold": "source.qty_sold",
        "sales_date": "source.sales_date"
    }
).execute()

# Optimize and vacuum Delta table
spark.sql(f"OPTIMIZE {output_table} ZORDER BY (product_id)")
spark.sql(f"VACUUM {output_table} RETAIN 0 HOURS")

# Perform data quality checks
df_with_checks = df.withColumn("is_valid", F.when(df["qty_sold"].isNull(), F.lit(False)).otherwise(F.lit(True)))
invalid_records = df_with_checks.filter(df_with_checks["is_valid"] == False).count()

# Implement caching strategy
df.unpersist()
df.cache()

# Error handling and logging (basic example)
try:
    # Simulate data processing step
    df_processed = df.withColumn("discounted_qty", df["qty_sold"] * 0.9)
except Exception as e:
    print(f"Error encountered: {e}")

# Stop the Spark session
spark.stop()

