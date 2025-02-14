from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
from pyspark.sql.functions import lit

# Initialize Spark session
spark = SparkSession.builder.appName("TestDataGeneration").getOrCreate()

# Define schema for sample_users_data
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", DateType(), True),
    StructField("valid", IntegerType(), True)
])

# Generate test data
data = [
    # Happy path test data
    ("US", "P1001", 50, "2024-01-15", 1),
    ("CA", "P1003", 25, "2024-01-17", 1),
    
    # Edge cases
    ("US", "P0000", 0, "2024-01-01", 1),  # Boundary qty_sold
    ("CA", "P9999", 1000000, "2024-12-31", 1),  # Large qty_sold
    
    # Error cases
    ("USA", "P1001", 50, "2024-01-15", 1),  # Invalid country_cd
    ("US", "1001", 50, "2024-01-15", 1),  # Invalid product_id
    ("US", "P1001", -10, "2024-01-15", 1),  # Negative qty_sold
    ("US", "P1001", 50, "15-01-2024", 1),  # Invalid sales_date format
    
    # NULL handling scenarios
    (None, "P1001", 50, "2024-01-15", 1),
    ("US", None, 50, "2024-01-15", 1),
    ("US", "P1001", None, "2024-01-15", 1),
    ("US", "P1001", 50, None, 1),
    ("US", "P1001", 50, "2024-01-15", None),
    
    # Special characters and multi-byte characters
    ("JP", "P1001", 50, "2024-01-15", 1),
    ("CN", "P1001", 50, "2024-01-15", 1),
    ("US", "P1001", 50, "2024-01-15", 1),
    ("US", "P1001", 50, "2024-01-15", 1),
    ("US", "P1001", 50, "2024-01-15", 1),
    ("US", "P1001", 50, "2024-01-15", 1),
    ("US", "P1001", 50, "2024-01-15", 1),
    ("US", "P1001", 50, "2024-01-15", 1),
    ("US", "P1001", 50, "2024-01-15", 1),
    ("US", "P1001", 50, "2024-01-15", 1)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show DataFrame
df.show(truncate=False)