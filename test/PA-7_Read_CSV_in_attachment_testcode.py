from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, when, expr, isnull
import unittest

# Initialize Spark session
spark = SparkSession.builder.appName("TestDatabricksEnvironment").getOrCreate()

# Define schema for sample sales data
expected_schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", TimestampType(), True)
])

# Sample data definition
test_data = [
    ("US", "P1001", 50, "2024-01-15"),
    ("CA", "P1003", 25, "2024-01-17"),
    ("UK", "P1004", 20, "2024-01-19"),
    ("IN", "P1001", 60, "2024-01-20"),
    ("", "P1001", 20, "2024-03-21")  # Error case: Missing country_cd
]

# Create a DataFrame
df = spark.createDataFrame(test_data, schema=["country_cd", "product_id", "qty_sold", "sales_date"])

# Test Class Definition
class DatabricksTest(unittest.TestCase):

    def test_schema_validation(self):
        schema = df.schema
        self.assertEqual(schema, expected_schema, "Schema should match the expected schema.")

    def test_data_type_conversion(self):
        df_converted = df.withColumn("qty_sold_str", col("qty_sold").cast(StringType()))
        self.assertTrue(df_converted.schema["qty_sold_str"].dataType == StringType(), "qty_sold should be convertible to StringType.")

    def test_data_entries_validation(self):
        valid_entry_count = df.where(
            (col("country_cd") != "") &
            (col("product_id").rlike("^P[0-9]{4}$")) &
            (col("qty_sold") > 0) &
            (expr("CAST(sales_date AS TIMESTAMP)").isNotNull())
        ).count()
        self.assertEqual(valid_entry_count, 4, "There should be 4 valid data entries out of 5.")

    def test_null_handling(self):
        # Check for NULLs in DataFrame
        null_count = df.select([when(isnull(c), c).alias(c) for c in df.columns]).where(
            col("country_cd").isNull() |
            col("product_id").isNull() |
            col("qty_sold").isNull() |
            col("sales_date").isNull()
        ).count()
        self.assertEqual(null_count, 0, "There should be no NULLs in the DataFrame.")

    def test_invalid_product_id(self):
        # Validate invalid product IDs
        invalid_product_id_count = df.where(~col("product_id").rlike("^P[0-9]{4}$")).count()
        self.assertEqual(invalid_product_id_count, 0, "There should be no invalid product_id matching.")

    def test_merge_update_delete_operations(self):
        # Delta table operations - A placeholder for Delta functions
        try:
            df.write.format("delta").mode("overwrite").save("/tmp/delta-table")
            delta_table = DeltaTable.forPath(spark, "/tmp/delta-table")
            
            # Simulating a MERGE operation
            delta_table.alias("tgt").merge(
                df.alias("src"),
                "tgt.product_id = src.product_id"
            ).whenMatchedUpdate(set={"qty_sold": expr("src.qty_sold")}).execute()
            
            # Simulating a DELETE operation
            delta_table.delete(col("qty_sold") < 0)
            
            # Validation that the operations succeed
            self.assertTrue(True, "Delta operations (MERGE, DELETE) should succeed.")
        except Exception as e:
            self.fail(f"Delta operations failed with exception: {e}")

# Execute the tests
unittest.main(argv=[''], verbosity=2, exit=False)
