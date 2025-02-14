# Databricks Test Code for Inventory at Risk Calculation

# Import libraries for PySpark testing
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, col
import pytest

# Create a Spark session for testing, if not already created
spark = SparkSession.builder \
    .appName("Databricks Inventory Test") \
    .getOrCreate()

# Sample Data Setup in Databricks SQL for testing
spark.sql("""
CREATE OR REPLACE TABLE agilisium_playground.purgo_playground.f_inv_movmnt (
    id BIGINT,
    dnsa_flag STRING,
    financial_qty DOUBLE,
    timestamp_event TIMESTAMP
);
""")

spark.sql("""
INSERT INTO agilisium_playground.purgo_playground.f_inv_movmnt VALUES
(1, 'Y', 150.0, TIMESTAMP('2024-03-21T00:00:00.000+0000')),
(2, 'N', 200.0, TIMESTAMP('2024-03-21T01:00:00.000+0000')),
(3, 'Y', 300.0, TIMESTAMP('2024-03-21T02:00:00.000+0000'));
""")

# PySpark Function to Calculate Inventory at Risk
def calculate_inventory_at_risk():
    # SQL to sum financial quantities where dnsa_flag is "Y"
    inventory_at_risk_result = spark.sql("""
        SELECT SUM(financial_qty) AS inventory_at_risk
        FROM agilisium_playground.purgo_playground.f_inv_movmnt
        WHERE dnsa_flag = 'Y'
    """)
    return inventory_at_risk_result.collect()[0][0]

# Unit Tests for Inventory at Risk Calculation
def test_calculate_inventory_at_risk():
    result = calculate_inventory_at_risk()
    # Verify the calculated sum
    assert result == 450.0, f"Expected 450.0 but got {result}"

# PySpark Function to Calculate Percentage of Inventory at Risk
def calculate_percentage_of_inventory_at_risk(inventory_at_risk, total_inventory):
    if total_inventory == 0:
        raise ValueError("Total inventory cannot be zero")
    percentage = (inventory_at_risk / total_inventory) * 100
    return percentage

# Unit Test for Percentage Calculation
def test_calculate_percentage_of_inventory_at_risk():
    inventory_at_risk = 450.0
    total_inventory = 1000.0
    percentage = calculate_percentage_of_inventory_at_risk(inventory_at_risk, total_inventory)
    assert percentage == 45.0, f"Expected 45.0 but got {percentage}"

    # Handling division by zero
    with pytest.raises(ValueError):
        calculate_percentage_of_inventory_at_risk(inventory_at_risk, 0)

# Cleanup Table after Tests
def test_cleanup_database():
    spark.sql("DROP TABLE IF EXISTS agilisium_playground.purgo_playground.f_inv_movmnt")

# Execute the tests using a test runner like pytest
# This section is intended to be run in a unit test framework, not directly in a notebook cell
if __name__ == "__main__":
    # Run the PySpark tests
    pytest.main([__file__])
