from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
from pyspark.sql.functions import lit

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TestDataGeneration") \
    .getOrCreate()

# Define schema for sample_users_data
users_schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", DateType(), True),
    StructField("valid", IntegerType(), True)
])

# Define schema for sample_sales_data
sales_schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", DateType(), True)
])

# Generate test data for sample_users_data
users_data = [
    # Happy path test data
    ("US", "P1001", 50, "2024-01-15", 1),
    ("US", "P1002", 30, "2024-01-16", 1),
    ("CA", "P1001", 40, "2024-01-15", 1),
    ("CA", "P1003", 25, "2024-01-17", 1),
    # Edge cases
    ("US", "P1004", 0, "2024-01-18", 1),  # Zero quantity
    ("US", "P1005", 2147483647, "2024-01-19", 1),  # Max int value
    # Error cases
    ("US", "P1006", -1, "2024-01-20", 1),  # Negative quantity
    ("US", "P1007", 30, "2024-01-21", 0),  # Invalid record
    # NULL handling scenarios
    (None, "P1008", 20, "2024-01-22", 1),  # Null country_cd
    ("US", None, 20, "2024-01-23", 1),  # Null product_id
    ("US", "P1009", None, "2024-01-24", 1),  # Null qty_sold
    ("US", "P1010", 20, None, 1),  # Null sales_date
    # Special characters and multi-byte characters
    ("JP", "P1011", 15, "2024-01-25", 1),  # Japanese country code
    ("US", "P1012", 20, "2024-01-26", 1),  # Special characters in product_id
]

# Create DataFrame for sample_users_data
users_df = spark.createDataFrame(users_data, schema=users_schema)

# Generate test data for sample_sales_data
sales_data = [
    # Happy path test data
    ("US", "P1001", 50, "2024-01-15"),
    ("US", "P1002", 30, "2024-01-16"),
    ("CA", "P1001", 40, "2024-01-15"),
    ("CA", "P1003", 25, "2024-01-17"),
    # Edge cases
    ("US", "P1004", 0, "2024-01-18"),  # Zero quantity
    ("US", "P1005", 2147483647, "2024-01-19"),  # Max int value
    # Error cases
    ("US", "P1006", -1, "2024-01-20"),  # Negative quantity
    # NULL handling scenarios
    (None, "P1008", 20, "2024-01-22"),  # Null country_cd
    ("US", None, 20, "2024-01-23"),  # Null product_id
    ("US", "P1009", None, "2024-01-24"),  # Null qty_sold
    ("US", "P1010", 20, None),  # Null sales_date
    # Special characters and multi-byte characters
    ("JP", "P1011", 15, "2024-01-25"),  # Japanese country code
    ("US", "P1012", 20, "2024-01-26"),  # Special characters in product_id
]

# Create DataFrame for sample_sales_data
sales_df = spark.createDataFrame(sales_data, schema=sales_schema)

# Show the generated test data
users_df.show(truncate=False)
sales_df.show(truncate=False)