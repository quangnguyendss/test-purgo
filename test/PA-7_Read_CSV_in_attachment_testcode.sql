-- Test SQL operations for sales data processing

-- Test case 1: Successful import to Unity Catalog
SELECT * FROM purgo_playground.sales_data;

-- Test case 2: Validate DataSchema
DESCRIBE purgo_playground.sales_data;

-- Unit Test SQL: Test individual transformation
SELECT product_id, SUM(qty_sold) AS total_qty
FROM purgo_playground.sales_data
GROUP BY product_id
HAVING SUM(qty_sold) IS NOT NULL;

-- Integration Test SQL: Validate entire flow
WITH expected_totals AS (
  SELECT 'P1001' AS product_id, 150 AS expected_qty
  UNION ALL
  SELECT 'P1002', 103
  UNION ALL
  SELECT 'P1003', 70
  UNION ALL
  SELECT 'P1004', 75
)
SELECT a.product_id, SUM(a.qty_sold) AS actual_qty, e.expected_qty
FROM purgo_playground.sales_data a
JOIN expected_totals e
ON a.product_id = e.product_id
GROUP BY a.product_id, e.expected_qty
HAVING SUM(a.qty_sold) = e.expected_qty;

-- NULL Handling Test
SELECT COUNT(*) AS null_country_count
FROM purgo_playground.sales_data
WHERE country_cd IS NULL;

-- Validate Delta Lake operations (MERGE/UPDATE/DELETE)
MERGE INTO purgo_playground.sales_data AS target
USING (SELECT 'US' AS country_cd, 'P1001' AS product_id) AS source
ON target.country_cd = source.country_cd AND target.product_id = source.product_id
WHEN MATCHED THEN
  UPDATE SET target.qty_sold = target.qty_sold + 10
WHEN NOT MATCHED THEN
  INSERT (country_cd, product_id, qty_sold, sales_date) VALUES ('US', 'P1001', 10, current_date());

-- Window Functions Test (Analytical feature)
SELECT country_cd, product_id, qty_sold,
  ROW_NUMBER() OVER (PARTITION BY country_cd ORDER BY qty_sold DESC) AS rank
FROM purgo_playground.sales_data;

-- Cleanup operation
TRUNCATE TABLE purgo_playground.sales_data;



# PySpark testing with pytest

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
from pyspark.sql import functions as F
import pytest

# Initialize Spark session
spark = SparkSession.builder.appName("TestDataSuite").getOrCreate()

# Define expected schema
expected_schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", DateType(), True)
])

# Global test data setup
@pytest.fixture(scope="module")
def sales_data():
    data = [("US", "P1001", 50, "2024-01-15"),
            ("US", "P1002", 30, "2024-01-16"),
            ("CA", "P1001", 40, "2024-01-15"),
            ("CA", "P1003", 25, "2024-01-17")]
    return spark.createDataFrame(data, expected_schema)

def test_data_schema(sales_data):
    assert sales_data.schema == expected_schema

def test_data_type_conversions(sales_data):
    transformed_df = sales_data.withColumn("qty_double", sales_data["qty_sold"].cast("double"))
    assert transformed_df.schema["qty_double"].dataType == DoubleType()

def test_null_handling():
    null_data = [(None, "P1002", 20, "2024-01-17"), 
                 ("IN", "P1003", None, "2024-01-21")]
    null_df = spark.createDataFrame(null_data, expected_schema)
    null_count = null_df.filter(F.col("country_cd").isNull() | F.col("qty_sold").isNull()).count()
    assert null_count == 2

def test_data_aggregation(sales_data):
    agg_df = sales_data.groupBy("product_id").agg(F.sum("qty_sold").alias("total_qty"))
    result = agg_df.collect()
    expected_result = [("P1001", 90), ("P1002", 30), ("P1003", 25)]
    assert result == expected_result

# Handle cleanup (stop Spark session)
def teardown_function():
    spark.stop()
