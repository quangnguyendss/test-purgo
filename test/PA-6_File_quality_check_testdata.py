from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count, lit, to_date, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# Spark session setup
spark = SparkSession.builder \
    .appName("File Quality Check and Data Load") \
    .getOrCreate()

# Define source and target paths
source_path = "dbfs:/FileStore/tables/sales_20240611.csv"
target_table = "purgo_playground.sales"
exception_table = "purgo_playground.exception"

# Define schema for reading the sales file
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("qty_sold", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("sale_date", StringType(), True)
])

# Load the CSV file into a DataFrame with schema
df_sales = spark.read.option("header", True).schema(schema).csv(source_path)

# Define validation checks
df_sales_valid = df_sales \
    .withColumn("country_cd_check", when(col("country_cd").isNull(), lit("country_cd should not be null")).otherwise(lit(None))) \
    .withColumn("qty_sold_check", when(~col("qty_sold").rlike("^[0-9]+$"), lit("qty_sold should be numeric")).otherwise(lit(None))) \
    .withColumn("product_id_check", lit(None)) \
    .withColumn("sale_date_check", when(~to_date(col("sale_date"), "yyyy-MM-dd").isNotNull(), lit("Incorrect date format")).otherwise(lit(None)))

# Identify duplicates for 'product_id'
windowSpec = Window.partitionBy("product_id")
df_duplicates = df_sales.groupBy("product_id").count().filter("count > 1").select("product_id")

# Update product_id_check
df_sales_valid = df_sales_valid \
    .join(df_duplicates.withColumn('product_id_check', lit('Duplicate product_id detected')), "product_id", "left") \
    .select("country_cd", "qty_sold", "product_id", "sale_date", "country_cd_check", "qty_sold_check", "product_id_check", "sale_date_check")

# Filter to identify erroneous records
df_error_records = df_sales_valid.filter("country_cd_check IS NOT NULL OR qty_sold_check IS NOT NULL OR product_id_check IS NOT NULL OR sale_date_check IS NOT NULL") \
    .withColumn("error_description", concat_ws("; ", col("country_cd_check"), col("qty_sold_check"), col("product_id_check"), col("sale_date_check")))

# Write erroneous records to exception table
df_error_records.select("country_cd", "qty_sold", "product_id", "sale_date", "error_description") \
    .write.option("mergeSchema", "true").mode("overwrite").format("parquet").saveAsTable(exception_table)

# Filter valid records
df_valid_records = df_sales_valid.filter("country_cd_check IS NULL AND qty_sold_check IS NULL AND product_id_check IS NULL AND sale_date_check IS NULL") \
    .select("country_cd", "qty_sold", "product_id", "sale_date")

# Load valid records to target sales table
df_valid_records.write.option("mergeSchema", "true").mode("overwrite").format("parquet").saveAsTable(target_table)