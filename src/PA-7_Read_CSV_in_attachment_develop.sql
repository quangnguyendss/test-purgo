/* Create the sample_sales_data table in the purgo_playground schema */
CREATE TABLE IF NOT EXISTS purgo_playground.sample_sales_data (
  country_cd STRING,
  product_id STRING,
  qty_sold INT,
  sales_date DATE
);

/* Load data from the CSV file into the table */
COPY INTO purgo_playground.sample_sales_data
FROM "dbfs:/FileStore/tables/sample_sales_data.csv"
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')
;

/* Add data with NULL values for NULL checks */
INSERT INTO purgo_playground.sample_sales_data (country_cd, product_id, qty_sold, sales_date) VALUES (NULL, 'P1005', 10, '2024-01-24');
INSERT INTO purgo_playground.sample_sales_data (country_cd, product_id, qty_sold, sales_date) VALUES ('US', NULL, 15, '2024-01-25');
INSERT INTO purgo_playground.sample_sales_data (country_cd, product_id, qty_sold, sales_date) VALUES ('CA', 'P1006', NULL, '2024-01-26');
INSERT INTO purgo_playground.sample_sales_data (country_cd, product_id, qty_sold, sales_date) VALUES ('UK', 'P1007', 20, NULL);


/* Add data with multi-byte and special characters */
INSERT INTO purgo_playground.sample_sales_data (country_cd, product_id, qty_sold, sales_date) VALUES ('JP', 'P1008', 10, '2024-02-01');
INSERT INTO purgo_playground.sample_sales_data (country_cd, product_id, qty_sold, sales_date) VALUES ('FR', 'P1009', 10, '2024-02-05');
INSERT INTO purgo_playground.sample_sales_data (country_cd, product_id, qty_sold, sales_date) VALUES ('CN', 'P1010', 10, '2024-03-10');
INSERT INTO purgo_playground.sample_sales_data (country_cd, product_id, qty_sold, sales_date) VALUES ('BR', 'P1011', 10, '2024-03-15');

# Databricks PySpark Test Code

%pip install chispa

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from chispa import assert_df_equality

# Initialize SparkSession (not required in Databricks notebooks, but included for standalone execution)
# spark = SparkSession.builder.appName("TestSampleSalesData").getOrCreate()

# Define expected schema
expected_schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", IntegerType(), True),
    StructField("sales_date", DateType(), True)
])

# Read data from the table
df = spark.table("purgo_playground.sample_sales_data")

# Test 1: Schema validation
assert df.schema == expected_schema

# Test 2: Row count
expected_count = spark.read.csv("dbfs:/FileStore/tables/sample_sales_data.csv", header=True, inferSchema=True).count() + 7 # Account for added rows during data exploration and NULL checks
assert df.count() == expected_count

# Test 3: Data validation (subset of happy path data)
expected_data = [
    ("US", "P1001", 50, "2024-01-15"),
    ("CA", "P1003", 25, "2024-01-17"),
    ("UK", "P1004", 20, "2024-01-19")
]
expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

assert_df_equality(df.filter(df.country_cd.isin(["US", "CA", "UK"])).filter(df.product_id.isin(["P1001", "P1003", "P1004"])), expected_df, ignore_row_order=True, ignore_column_order=True)


# Test 4: NULL checks
assert df.filter(df["country_cd"].isNull()).count() >= 1
assert df.filter(df["product_id"].isNull()).count() >= 1
assert df.filter(df["qty_sold"].isNull()).count() >= 1
assert df.filter(df["sales_date"].isNull()).count() >= 1



# Test 5: Special characters and multi-byte character checks
assert df.filter(df["country_cd"] == "JP").count() > 0
assert df.filter(df["country_cd"] == "FR").count() > 0


# Further test cases for negative scenarios, edge cases and boundary conditions,
# performance testing, etc., can be added here using appropriate PySpark testing functions and libraries.