import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from datetime import datetime

class TestFileQualityCheck(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize Spark session
        cls.spark = SparkSession.builder \
            .appName("FileQualityCheckTest") \
            .master("local") \
            .getOrCreate()

        # Define schema for sales data
        cls.sales_schema = StructType([
            StructField("country_cd", StringType(), True),
            StructField("qty_sold", IntegerType(), True),
            StructField("product_id", StringType(), True),
            StructField("Date", StringType(), True)
        ])

        # Define schema for exception table
        cls.exception_schema = StructType([
            StructField("country_cd", StringType(), True),
            StructField("qty_sold", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("Date", StringType(), True),
            StructField("error_message", StringType(), True)
        ])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_happy_path_data(self):
        # Test valid records
        data = [
            {"country_cd": "US", "qty_sold": 100, "product_id": "P12345", "Date": "2023-10-01"},
            {"country_cd": "CA", "qty_sold": 200, "product_id": "P12346", "Date": "2023-10-02"},
            {"country_cd": "GB", "qty_sold": 150, "product_id": "P12347", "Date": "2023-10-03"},
        ]
        df = self.spark.createDataFrame(data, schema=self.sales_schema)
        self.assertEqual(df.count(), 3)
        self.assertTrue(df.filter(col("country_cd").isNull()).count() == 0)
        self.assertTrue(df.filter(~col("qty_sold").cast("int").isNotNull()).count() == 0)
        self.assertTrue(df.select("product_id").distinct().count() == df.count())
        self.assertTrue(df.filter(~col("Date").rlike(r"^\d{4}-\d{2}-\d{2}$")).count() == 0)

    def test_edge_case_data(self):
        # Test edge cases
        data = [
            {"country_cd": "AU", "qty_sold": 0, "product_id": "P12348", "Date": "2023-10-04"},
            {"country_cd": "IN", "qty_sold": 999999, "product_id": "P12349", "Date": "2023-10-05"},
            {"country_cd": "FR", "qty_sold": 300, "product_id": "P12350", "Date": "2023-12-31"},
        ]
        df = self.spark.createDataFrame(data, schema=self.sales_schema)
        self.assertEqual(df.count(), 3)
        self.assertTrue(df.filter(col("qty_sold") < 0).count() == 0)
        self.assertTrue(df.filter(col("qty_sold") > 999999).count() == 0)

    def test_error_case_data(self):
        # Test error cases
        data = [
            {"country_cd": None, "qty_sold": 100, "product_id": "P12351", "Date": "2023-10-06"},
            {"country_cd": "DE", "qty_sold": "abc", "product_id": "P12352", "Date": "2023-10-07"},
            {"country_cd": "JP", "qty_sold": 400, "product_id": "P12345", "Date": "2023-10-08"},
            {"country_cd": "BR", "qty_sold": 500, "product_id": "P12353", "Date": "10-09-2023"},
        ]
        df = self.spark.createDataFrame(data, schema=self.sales_schema)
        self.assertTrue(df.filter(col("country_cd").isNull()).count() > 0)
        self.assertTrue(df.filter(~col("qty_sold").cast("int").isNotNull()).count() > 0)
        self.assertTrue(df.select("product_id").distinct().count() < df.count())
        self.assertTrue(df.filter(~col("Date").rlike(r"^\d{4}-\d{2}-\d{2}$")).count() > 0)

    def test_special_character_data(self):
        # Test special character handling
        data = [
            {"country_cd": "U$@", "qty_sold": 600, "product_id": "P12354", "Date": "2023-10-10"},
            {"country_cd": "IT", "qty_sold": 700, "product_id": "P@#123", "Date": "2023-10-11"},
            {"country_cd": "ES", "qty_sold": 800, "product_id": "P12355", "Date": "2023/10/12"},
        ]
        df = self.spark.createDataFrame(data, schema=self.sales_schema)
        self.assertTrue(df.filter(~col("country_cd").rlike(r"^[A-Z]{2}$")).count() > 0)
        self.assertTrue(df.filter(~col("product_id").rlike(r"^P\d+$")).count() > 0)
        self.assertTrue(df.filter(~col("Date").rlike(r"^\d{4}-\d{2}-\d{2}$")).count() > 0)

if __name__ == '__main__':
    unittest.main()
