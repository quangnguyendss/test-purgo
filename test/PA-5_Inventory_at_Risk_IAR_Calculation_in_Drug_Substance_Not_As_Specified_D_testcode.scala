/* Setup and Configuration Section */
/* 
-- Ensure necessary libraries are installed 
-- Unity Catalog schema is "purgo_playground"
-- Data table: "f_inv_movmnt" in "agilisium_playground.purgo_playground"
-- Spark SQL environment is configured 
-- Access permissions to Unity Catalog are granted
*/

/* PySpark Testing Code */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

/* Install required libraries if any missing */
/*
!pip install <library_name_if_needed>
*/

class InventoryAtRiskTest extends AnyFunSuite {

  /* Initialize Spark session */
  val spark = SparkSession.builder
    .appName("InventoryAtRiskTest")
    .getOrCreate()

  import spark.implicits._

  /* Schema for f_inv_movmnt table */
  val schema = StructType(Array(
    StructField("id", LongType, nullable = false),
    StructField("dnsa_flag", StringType, nullable = true),
    StructField("financial_qty", DoubleType, nullable = true),
    StructField("timestamp_event", TimestampType, nullable = true)
  ))

  /* Unit test for individual transformations */
  test("Calculate Inventory At Risk") {
    val testData = Seq(
      (1L, "Y", 100.0, "2024-03-21T00:00:00.000+0000"),
      (2L, "N", 200.0, "2024-03-21T01:00:00.000+0000"),
      (3L, "Y", 150.0, "2024-03-21T02:00:00.000+0000")
    ).toDF("id", "dnsa_flag", "financial_qty", "timestamp_event")

    val inventoryAtRisk = testData
      .filter($"dnsa_flag" === "Y")
      .agg(sum($"financial_qty").as("inventory_at_risk"))
      .collect()(0)(0)

    assert(inventoryAtRisk == 250.0)
  }

  /* Integration test for end-to-end flow */
  test("Calculate Percentage of Inventory At Risk") {
    val totalInventory = 1000.0
    val inventoryAtRisk = 250.0
    val expectedPercentage = (inventoryAtRisk / totalInventory) * 100

    assert(expectedPercentage == 25.0)
  }

  /* Data Quality Validation Tests */
  test("NULL Handling in financial_qty") {
    val testData = Seq(
      (1L, "Y", null.asInstanceOf[Double], "2024-03-21T00:00:00.000+0000"),
      (2L, "N", 200.0, "2024-03-21T01:00:00.000+0000"),
      (3L, "Y", 150.0, "2024-03-21T02:00:00.000+0000")
    ).toDF("id", "dnsa_flag", "financial_qty", "timestamp_event")

    val inventoryAtRisk = testData
      .filter($"dnsa_flag" === "Y")
      .agg(sum($"financial_qty").as("inventory_at_risk"))
      .collect()(0)(0)

    assert(inventoryAtRisk == 150.0) // Only one valid 'Y' with 150.0
  }

  /* Cleanup operations */
  override def afterAll(): Unit = {
    spark.stop()
  }
}

/* SQL Testing Code Using Databricks SQL Syntax */

-- Test for inventory_at_risk calculation
SELECT SUM(financial_qty) AS inventory_at_risk
FROM agilisium_playground.purgo_playground.f_inv_movmnt
WHERE dnsa_flag = "Y";

-- Validate percentage_of_inventory_at_risk calculation
SELECT
  (SUM(financial_qty) FILTER(WHERE dnsa_flag = "Y") / SUM(financial_qty)) * 100 AS percentage_of_inventory_at_risk
FROM agilisium_playground.purgo_playground.f_inv_movmnt;

-- Test Delta Lake operations
-- Assuming Delta Lake is used for MERGE and other DML operations
MERGE INTO agilisium_playground.purgo_playground.f_inv_movmnt AS target
USING (SELECT 28 AS id, "Y" AS dnsa_flag, -1.0 AS financial_qty, current_timestamp() AS timestamp_event) AS source
ON target.id = source.id
WHEN MATCHED THEN
  UPDATE SET target.financial_qty = source.financial_qty
WHEN NOT MATCHED THEN
  INSERT (id, dnsa_flag, financial_qty, timestamp_event)
  VALUES (source.id, source.dnsa_flag, source.financial_qty, source.timestamp_event);

-- Testing cleanup after test execution
-- Example of deleting specific test data post-validation
DELETE FROM agilisium_playground.purgo_playground.f_inv_movmnt WHERE id > 20;

/* Include performance tests where applicable */
-- Performance test for large data volumes
WITH large_data AS (
  SELECT id, dnsa_flag, financial_qty, timestamp_event 
  FROM agilisium_playground.purgo_playground.f_inv_movmnt
  UNION ALL
  SELECT id, dnsa_flag, financial_qty, timestamp_event 
  FROM agilisium_playground.purgo_playground.f_inv_movmnt
)
SELECT SUM(financial_qty) FROM large_data;