# Databricks Setup and Required Libraries
# Ensure necessary imports and installations

# Install required packages for PySpark testing
# MAGIC %pip install pytest

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import pytest

# Create Spark session for PySpark unit tests
spark = SparkSession.builder \
    .appName("Databricks Test Suite") \
    .getOrCreate()

# Schema setup for SQL test table
schema_f_inv_movmnt = StructType([
    StructField("id", LongType(), True),
    StructField("dnsa_flag", StringType(), True),
    StructField("financial_qty", DoubleType(), True),
    StructField("timestamp_event", TimestampType(), True)
])

# Define creating test data for SQL table
test_data = [
    Row(id=1, dnsa_flag="Y", financial_qty=150.0, timestamp_event="2024-03-21T00:00:00.000+0000"),
    # Add more test data as needed following the test data specified
]

# Create DataFrame for testing
df_f_inv_movmnt = spark.createDataFrame(test_data, schema=schema_f_inv_movmnt)

# Temporary view for SQL queries
df_f_inv_movmnt.createOrReplaceTempView("f_inv_movmnt")

# Example unit test function using Pytest
def test_inventory_at_risk_calculation():
    # SQL to calculate inventory at risk when dns_flag is "Y"
    result = spark.sql("""
        SELECT SUM(financial_qty) AS inventory_at_risk 
        FROM f_inv_movmnt 
        WHERE dnsa_flag = "Y"
    """).collect()[0].inventory_at_risk

    # Expected result based on the test data
    expected_inventory_at_risk = 450.0 # Replace with appropriate value from test data
    
    # Assertion to check if the calculated value matches expected
    assert result == expected_inventory_at_risk, f"Expected {expected_inventory_at_risk} but got {result}"

# Streaming test function (example)    
@pytest.mark.parametrize("batch_data,expected_risk", [
    ([(2, "Y", 250.0)], 1070.0),  # Example where additional data pushes risk
    ([], 820.0),                   # No additional data
])
def test_streaming_inventory_at_risk(batch_data, expected_risk):
    # Create streaming DataFrame
    schema = schema_f_inv_movmnt
    stream_df = spark.createDataFrame(batch_data, schema)
    
    # Perform streaming test logic here
    result = stream_df.filter(col("dnsa_flag") == "Y").groupBy().sum("financial_qty").collect()[0][0]
    
    # Assert the result matches expected risk
    assert result == expected_risk, "Streaming inventory at risk mismatch"

# SQL for cleanup (DROP TABLE) after tests
cleanup_sql = """
DROP TABLE IF EXISTS agilisium_playground.purgo_playground.f_inv_movmnt
"""
spark.sql(cleanup_sql)

# Additional PySpark test function examples
def test_data_schema_validation():
    # Validate schema of f_inv_movmnt
    expected_schema = schema_f_inv_movmnt
    
    # Get schema from the DataFrame
    actual_schema = df_f_inv_movmnt.schema
    
    # Ensure the DataFrame's schema matches the expected schema
    assert actual_schema == expected_schema, "Schema validation failed"

@pytest.mark.parametrize("flag, expected", [
    ("Y", True),
    ("N", False),
    (None, False),
])
def test_flag_active_conditions(flag, expected):
    # Logic to test flag_active conditions
    result = flag == "Y"
    assert result == expected, f"{flag} did not return {expected}"

# Pytest options for running tests in Databricks
if __name__ == "__main__":
    pytest.main(["-v", __file__])

