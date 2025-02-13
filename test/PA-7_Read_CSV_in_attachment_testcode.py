from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, sum as _sum
import unittest

class TestDataProcessing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestDataProcessing").getOrCreate()

        # Define the schema based on the specifications
        cls.schema = StructType([
            StructField("country_cd", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("qty_sold", IntegerType(), True),
            StructField("sales_date", DateType(), True)
        ])

        # Sample data for validation
        cls.data = [
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

        cls.df = cls.spark.createDataFrame(cls.data, schema=cls.schema)
        cls.df.createOrReplaceTempView("sales_data")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_schema_validation(self):
        expected_schema = StructType([
            StructField("country_cd", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("qty_sold", IntegerType(), True),
            StructField("sales_date", DateType(), True)
        ])
        self.assertEqual(self.df.schema, expected_schema)

    def test_data_type_conversions(self):
        converted_df = self.df.withColumn("sales_date", col("sales_date").cast(DateType()))
        self.assertEqual(converted_df.schema["sales_date"].dataType, DateType())

    def test_null_handling(self):
        null_df = self.df.filter(col("country_cd").isNull() | col("qty_sold").isNull())
        self.assertEqual(null_df.count(), 0)

    def test_total_sales_per_product(self):
        result_df = self.spark.sql("""
            SELECT product_id, SUM(qty_sold) as total_qty_sold
            FROM sales_data
            GROUP BY product_id
        """)
        expected_data = [("P1001", 150), ("P1002", 103), ("P1003", 70), ("P1004", 75)]
        expected_df = self.spark.createDataFrame(expected_data, schema=["product_id", "total_qty_sold"])

        result_rows = sorted(result_df.collect(), key=lambda row: row['product_id'])
        expected_rows = sorted(expected_df.collect(), key=lambda row: row['product_id'])
        self.assertEqual(result_rows, expected_rows)

    def test_delta_lake_operations(self):
        delta_table = "purgo_playground.sales_data_delta"
        self.df.write.format("delta").mode("overwrite").saveAsTable(delta_table)

        read_df = self.spark.table(delta_table)
        self.assertEqual(read_df.count(), self.df.count())

        self.spark.sql(f"DELETE FROM {delta_table} WHERE country_cd = 'US'")
        delete_read_df = self.spark.table(delta_table)
        self.assertEqual(delete_read_df.count(), self.df.count() - 2)

    def test_merge_operation(self):
        delta_table = "purgo_playground.sales_data_merge"
        self.df.write.format("delta").mode("overwrite").saveAsTable(delta_table)

        merge_data = [("US", "P1001", 70, "2024-01-15")]
        merge_df = self.spark.createDataFrame(merge_data, schema=self.schema)
        merge_df.createOrReplaceTempView("merge_data")

        self.spark.sql(f"""
            MERGE INTO {delta_table} AS target
            USING merge_data AS source
            ON target.country_cd = source.country_cd AND target.product_id = source.product_id
            WHEN MATCHED THEN UPDATE SET target.qty_sold = source.qty_sold
            WHEN NOT MATCHED THEN INSERT *
        """)

        result_df = self.spark.sql(f"SELECT * FROM {delta_table} WHERE country_cd = 'US' AND product_id = 'P1001'")
        self.assertEqual(result_df.collect()[0]["qty_sold"], 70)

if __name__ == "__main__":
    unittest.main(argv=[''], verbosity=2, exit=False)
