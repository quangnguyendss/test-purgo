from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, sum as Fsum, regexp_extract, expr
import unittest

spark = SparkSession.builder \
    .appName("Databricks Test") \
    .enableHiveSupport() \
    .getOrCreate()

schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", StringType(), True)
])

# Sample data loading for testing
data = [
    ("US", "P1001", 50, "2024-01-15"),
    ("US", "P1002", 30, "2024-01-16"),
    ("CA", "P1001", 40, "2024-01-15"),
    ("CA", "P1003", 25, "2024-01-17"),
    ("UK", "P1002", 35, "2024-01-18"),
    ("UK", "P1004", 20, "2024-01-19"),
    ("IN", "P1001", 60, "2024-01-20"),
    ("IN", "P1003", 45, "2024-01-21"),
    ("AU", "P1004", 55, "2024-01-22"),
    ("AU", "P1002", 38, "2024-01-23"),
]

df = spark.createDataFrame(data, schema)

class TestPurgoPlayground(unittest.TestCase):

    def test_country_code_format(self):
        invalid_country = df.where(~col("country_cd").rlike("^[A-Z]{2}$"))
        self.assertTrue(invalid_country.count() == 0, "Invalid country_cd format found")

    def test_product_id_format(self):
        invalid_product_id = df.where(~col("product_id").rlike("^P\d{4}$"))
        self.assertTrue(invalid_product_id.count() == 0, "Invalid product_id format found")

    def test_positive_qty_sold(self):
        negative_qty_sold = df.where(col("qty_sold") <= 0)
        self.assertTrue(negative_qty_sold.count() == 0, "Negative qty_sold found")

    def test_sales_date_format(self):
        invalid_sales_date = df.where(~col("sales_date").rlike("^\d{4}-\d{2}-\d{2}$"))
        self.assertTrue(invalid_sales_date.count() == 0, "Invalid sales_date format found")

    def test_total_quantity_by_product(self):
        expected_totals = {
            "P1001": 150,
            "P1002": 103,
            "P1003": 70,
            "P1004": 75
        }
        calculated_totals = df.groupBy("product_id").agg(Fsum("qty_sold").alias("total_qty")).collect()
        for row in calculated_totals:
            product_id = row["product_id"]
            total_qty = row["total_qty"]
            self.assertEqual(total_qty, expected_totals[product_id])

    def test_no_future_dates(self):
        future_dates = df.where(expr("sales_date > current_date()"))
        self.assertTrue(future_dates.count() == 0, "Future date found in sales_date")

    def test_null_handling(self):
        null_checks = df.select([col(c).isNull().alias(c + "_is_null") for c in df.columns])
        self.assertTrue(null_checks.rdd.map(lambda row: all(v == False for v in row)).reduce(lambda a, b: a and b), "NULL value found in DataFrame")

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)  # Don't call sys.exit() in Jupyter
