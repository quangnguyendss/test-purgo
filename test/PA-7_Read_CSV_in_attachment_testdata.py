from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestDataGeneration").getOrCreate()

# Schema definition
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", DateType(), True)
])

# Happy path test data
happy_path_data = [
    ("US", "P1001", 50, "2024-01-15"),
    ("US", "P1002", 30, "2024-01-16"),
    ("CA", "P1001", 40, "2024-01-15"),
    ("CA", "P1003", 25, "2024-01-17"),
    ("UK", "P1002", 35, "2024-01-18"),
    ("UK", "P1004", 20, "2024-01-19"),
    ("IN", "P1001", 60, "2024-01-20"),
    ("IN", "P1003", 45, "2024-01-21"),
    ("AU", "P1004", 55, "2024-01-22"),
    ("AU", "P1002", 38, "2024-01-23")
]

# Edge cases (boundary conditions)
edge_cases_data = [
    ("US", "P9999", Integer.MAX_VALUE , "2024-01-01"), # Max qty_sold
    ("CA", "P0001", 0, "2024-12-31"), # Min qty_sold
    ("UK", "P1005", 1, "2024-02-29"), # Leap year date

]

# Error cases
error_cases_data = [
    ("US", "P1006", -1, "2024-01-15"), # Invalid negative quantity
    (None, "P1005", 20, "2024-01-16")   # Missing country code
]

# Null handling scenarios
null_data = [
    ("US", "P1001", None, "2024-01-15"),
    ("CA", None, 40, "2024-01-15"),
    ("UK", "P1002", 35, None)
]


# Special characters and multi-byte characters
special_char_data = [
    ("JP", "製品1001", 50, "2024-01-15"),  # Multi-byte characters
    ("FR", "Produit-Spécial", 30, "2024-01-16") # Special characters
]



# Combine all data
all_data = happy_path_data + edge_cases_data + error_cases_data + null_data + special_char_data


# Create DataFrame
df = spark.createDataFrame(data=all_data, schema=schema)

# Write to Databricks table
df.write.mode("overwrite").saveAsTable("purgo_playground.sample_sales_data")