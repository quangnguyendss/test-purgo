# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, lit, expr
from pyspark.sql.utils import AnalysisException
import unittest

# /* Spark session initialization */
spark = SparkSession.builder \
    .appName("Databricks Testing") \
    .enableHiveSupport() \
    .getOrCreate()

# /* Setting configurations for accessing Unity Catalog */
# spark.conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse")
# spark.conf.set("spark.sql.catalogImplementation", "hive")

# /* Utility function to drop tables to ensure clean test environment */
def drop_table_if_exists(table_name):
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    except AnalysisException as e:
        pass  # If table doesn't exist, we pass

# Setup and teardown methods for test cases
class InventoryAtRiskTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Connect to or create the test data table
        drop_table_if_exists("agilisium_playground.purgo_playground.f_inv_movmnt")

        # Create table and insert records for testing
        spark.sql("""
        CREATE TABLE agilisium_playground.purgo_playground.f_inv_movmnt (
            id BIGINT,
            dnsa_flag STRING,
            financial_qty DOUBLE,
            timestamp_event TIMESTAMP
        ) USING DELTA
        """)

        # Insert test data into the table
        spark.sql("""
        INSERT INTO agilisium_playground.purgo_playground.f_inv_movmnt VALUES 
        (1, "Y", 100.0, TIMESTAMP('2024-03-21T00:00:00.000+0000')), 
        (2, "Y", 200.0, TIMESTAMP('2024-03-21T01:00:00.000+0000')),
        (3, "N", 300.0, TIMESTAMP('2024-03-21T02:00:00.000+0000'))
        """)

    @classmethod
    def tearDownClass(cls):
        # Clean up the test data after all tests
        drop_table_if_exists("agilisium_playground.purgo_playground.f_inv_movmnt")

    def test_calculate_inventory_at_risk(self):
        # /* SQL query to calculate inventory_at_risk */
        query = """
        SELECT SUM(financial_qty) AS inventory_at_risk 
        FROM agilisium_playground.purgo_playground.f_inv_movmnt 
        WHERE dnsa_flag = 'Y'
        """
        result_df = spark.sql(query)
        result_row = result_df.collect()[0]
        self.assertEqual(result_row['inventory_at_risk'], 300.0)

    def test_percentage_inventory_at_risk(self):
        # Assuming we have total_inventory from an external source
        total_inventory = 1000.0

        # /* SQL to compute percentage of inventory at risk */
        query = f"""
        SELECT 
            (SUM(CASE WHEN dnsa_flag = 'Y' THEN financial_qty ELSE 0 END) / {total_inventory} * 100) 
            AS percentage_inventory_at_risk 
        FROM agilisium_playground.purgo_playground.f_inv_movmnt
        """
        result_df = spark.sql(query)
        result_row = result_df.collect()[0]
        # Verification for calculation
        self.assertAlmostEqual(result_row['percentage_inventory_at_risk'], 30.0)

    def test_handle_null_scenario(self):
        # Verify the behavior when critical fields like financial_qty are NULL
        query = """
        SELECT COUNT(*) AS cnt 
        FROM agilisium_playground.purgo_playground.f_inv_movmnt 
        WHERE financial_qty IS NULL
        """
        result_df = spark.sql(query)
        result_row = result_df.collect()[0]
        self.assertEqual(result_row['cnt'], 0)  # Change expected value based on your data

    def test_data_type_conversion(self):
        # Test data type conversions using SQL expressions
        df = spark.table("agilisium_playground.purgo_playground.f_inv_movmnt")
        df = df.withColumn("financial_qty_string", df["financial_qty"].cast("STRING"))
        self.assertEqual(df.schema["financial_qty_string"].dataType.simpleString(), "string")

    def test_delta_lake_operations(self):
        # Validate Delta Lake operations for MERGE
        df = spark.sql("SELECT * FROM agilisium_playground.purgo_playground.f_inv_movmnt")
        input_data = [(4, 'Y', 500.0, None)]
        input_schema = df.schema
        input_df = spark.createDataFrame(input_data, input_schema)
        
        # Perform MERGE operation
        input_df.createOrReplaceTempView("temp_table")
        spark.sql("""
        MERGE INTO agilisium_playground.purgo_playground.f_inv_movmnt AS target
        USING temp_table AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET target.financial_qty = source.financial_qty
        WHEN NOT MATCHED THEN INSERT *
        """)

        # Validate MERGE results
        merge_result = spark.sql("SELECT * FROM agilisium_playground.purgo_playground.f_inv_movmnt WHERE id = 4").collect()
        self.assertGreater(len(merge_result), 0)

if __name__ == '__main__':
    unittest.main(argv=[''], verbosity=2, exit=False)