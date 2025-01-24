from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.utils import AnalysisException

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FileQualityCheck") \
    .getOrCreate()

# Define schema for sales data
sales_schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("Date", StringType(), True)
])

# Define schema for exception table
exception_schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("qty_sold", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("error_message", StringType(), True)
])

# Load sales data from DBFS
sales_df = spark.read.csv("dbfs:/FileStore/tables/sales_20240611.csv", schema=sales_schema, header=True)

# Perform quality checks
exception_df = sales_df.withColumn("error_message", lit(None)) \
    .withColumn("error_message", when(col("country_cd").isNull(), "country_cd is null")
                .when(~col("qty_sold").cast("int").isNotNull(), "qty_sold is not numeric")
                .when(col("product_id").isNull(), "product_id is null")
                .when(~col("Date").rlike(r"^\d{4}-\d{2}-\d{2}$"), "Date is not in yyyy-mm-dd format")
                .otherwise(None)) \
    .filter(col("error_message").isNotNull())

# Check for duplicate product_id
duplicate_product_ids = sales_df.groupBy("product_id").count().filter("count > 1").select("product_id")
exception_df = exception_df.union(
    sales_df.join(duplicate_product_ids, "product_id", "inner")
    .withColumn("error_message", lit("Duplicate product_id"))
)

# Load exception records into exception table
try:
    exception_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("exception_table")
except AnalysisException as e:
    print(f"Error writing to exception table: {e}")

# Drop exception records from sales data
clean_sales_df = sales_df.subtract(exception_df.drop("error_message"))

# Load cleaned data into sales table
try:
    clean_sales_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("sales_table")
except AnalysisException as e:
    print(f"Error writing to sales table: {e}")

# Stop Spark session
spark.stop()
