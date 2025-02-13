from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, expr, when
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.utils import AnalysisException

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Databricks Test Data Validation") \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", TimestampType(), True)
])

# Load test DataFrame
df = spark.read.format("delta").table("purgo_playground.sample_sales_data")

# SQL Testing: Validate country_cd format
def test_country_cd_format():
    invalid_countries = df.filter(~col("country_cd").rlike("^[A-Z]{2}$"))
    assert invalid_countries.count() == 0, f"Invalid country codes found: {invalid_countries.show()}"

# SQL Testing: Validate product_id format
def test_product_id_format():
    invalid_products = df.filter(~col("product_id").rlike("^P\\d{4}$"))
    assert invalid_products.count() == 0, f"Invalid product_ids found: {invalid_products.show()}"

# SQL Testing: Validate qty_sold as positive integer
def test_qty_sold_positive():
    invalid_qtys = df.filter(col("qty_sold") <= 0)
    assert invalid_qtys.count() == 0, f"Non-positive quantities found: {invalid_qtys.show()}"

# SQL Testing: Validate correct date format for sales_date
def test_sales_date_format():
    try:
        df.withColumn("sales_date_cast", col("sales_date").cast("date")).count()
    except AnalysisException as e:
        assert False, f"Incorrect sales_date format: {e}"

# DataFrame Schema Validation
def test_schema():
    expected_schema = StructType([
        StructField("country_cd", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("qty_sold", IntegerType(), True),
        StructField("sales_date", TimestampType(), True)
    ])
    assert df.schema == expected_schema, f"Schema does not match. Expected: {expected_schema} Actual: {df.schema}"

# Delta Lake Operations Test
def test_delta_lake_operations():
    df.createOrReplaceTempView("test_delta")
    merge_query = """
    MERGE INTO purgo_playground.sample_sales_data TARGET
    USING test_delta SOURCE
    ON TARGET.product_id = SOURCE.product_id
    WHEN MATCHED THEN UPDATE SET TARGET.qty_sold = SOURCE.qty_sold + TARGET.qty_sold
    WHEN NOT MATCHED THEN INSERT *
    """
    spark.sql(merge_query)
    # Validate merge operation
    result_df = spark.read.format("delta").table("purgo_playground.sample_sales_data")
    assert result_df.count() == df.count(), "Mismatch in row count post MERGE operation"

# Window Function Test: Calculate total sales per product
def test_total_sales_per_product():
    from pyspark.sql.window import Window
    from pyspark.sql.functions import sum as sql_sum

    window_spec = Window.partitionBy("product_id")
    sales_df = df.withColumn("total_sales", sql_sum("qty_sold").over(window_spec))
    expected_totals = {
        'P1001': 150, 'P1002': 103, 'P1003': 70, 'P1004': 75
    }
    for product_id, total in expected_totals.items():
        actual_total = sales_df.filter(col("product_id") == product_id).select("total_sales").first()[0]
        assert actual_total == total, f"Incorrect total for product_id {product_id}: Expected {total}, Found {actual_total}"

# Streaming Test
def test_streaming():
    # Ingest as a streaming source
    streaming_df = spark.readStream.format("delta").table("purgo_playground.sample_sales_data")

    query = (
        streaming_df.writeStream
        .format("memory")  # MemoryStream for testing
        .queryName("sales_stream")
        .outputMode("append")
        .start()
    )

    query.awaitTermination(timeout=10)
    spark.sql("SELECT * FROM sales_stream").show()
    assert spark.sql("SELECT COUNT(*) FROM sales_stream").collect()[0][0] == df.count()
    query.stop()

# Cleanup Test Data
def cleanup_test_data():
    spark.sql("DELETE FROM purgo_playground.sample_sales_data WHERE TRUE")

# Run all tests
def run_tests():
    test_country_cd_format()
    test_product_id_format()
    test_qty_sold_positive()
    test_sales_date_format()
    test_schema()
    test_delta_lake_operations()
    test_total_sales_per_product()
    test_streaming()
    cleanup_test_data()

# Execute tests
try:
    run_tests()
    print("All tests passed successfully.")
except AssertionError as e:
    print(f"Test failed: {e}")
finally:
    spark.stop()

