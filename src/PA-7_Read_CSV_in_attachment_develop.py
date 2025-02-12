from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, sum as Fsum, current_date, expr, regexp_extract
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("Purgo Playground Data Processing") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.purgo_playground", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define the schema for the data
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", StringType(), True)
])

# Load the data
data_path = "path/to/sample_sales_data.csv"
df = spark.read.option("header", "true").schema(schema).csv(data_path)

# Data Validation
def validate_data(df):
    # Validate country code
    invalid_country = df.filter(~col("country_cd").rlike("^[A-Z]{2}$"))
    if invalid_country.count() > 0:
        raise ValueError("Invalid country_cd format found")

    # Validate product ID
    invalid_product_id = df.filter(~col("product_id").rlike("^P\d{4}$"))
    if invalid_product_id.count() > 0:
        raise ValueError("Invalid product_id format found")

    # Validate positive qty_sold
    negative_qty_sold = df.filter(col("qty_sold") <= 0)
    if negative_qty_sold.count() > 0:
        raise ValueError("Negative qty_sold found")

    # Validate sales_date format
    invalid_sales_date = df.filter(~col("sales_date").rlike("^\d{4}-\d{2}-\d{2}$"))
    if invalid_sales_date.count() > 0:
        raise ValueError("Invalid sales_date format found")

    # Validate no future dates
    future_dates = df.filter(expr("sales_date > current_date()"))
    if future_dates.count() > 0:
        raise ValueError("Future date found in sales_date")

validate_data(df)

# Data Processing - Calculate Total Quantity Sold Per Product
total_qty_df = df.groupBy("product_id").agg(Fsum("qty_sold").alias("total_qty"))

# Delta Lake Integration
delta_table_path = "path/to/delta_table"
delta_table = DeltaTable.forPath(spark, delta_table_path)

# Merge data into Delta table
delta_table.alias("existing").merge(
    df.alias("updates"),
    "existing.product_id = updates.product_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Optimize Delta Table by Z-Ordering on product_id
spark.sql(f"OPTIMIZE delta.`{delta_table_path}` ZORDER BY (product_id)")

# Vacuum Delta Table
spark.sql(f"VACUUM delta.`{delta_table_path}`")

# Caching for performance
df.cache()
total_qty_df.cache()
