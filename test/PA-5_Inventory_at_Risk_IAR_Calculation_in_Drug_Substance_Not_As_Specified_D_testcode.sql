/* Ensure necessary libraries are installed 
IF NEEDED */
-- CONFIGURE SQL QUERIES FOR TESTING IN DATABRICKS ENVIRONMENT

/* Initial setup: Create test data in Databricks */

CREATE OR REPLACE TABLE agilisium_playground.purgo_playground.f_inv_movmnt (
    id BIGINT,
    dnsa_flag STRING,
    financial_qty DOUBLE,
    timestamp_event TIMESTAMP
);

-- Insert diverse test records into the Databricks table
INSERT INTO agilisium_playground.purgo_playground.f_inv_movmnt VALUES
(1, "Y", 150.0, TIMESTAMP('2024-03-21T00:00:00.000+0000')),
(2, "N", 200.0, TIMESTAMP('2024-03-21T01:00:00.000+0000')),
(3, "Y", 300.0, TIMESTAMP('2024-03-21T02:00:00.000+0000')),
-- Additional test data as specified earlier
-- ...

/* SQL UNIT TESTS */

/* Test Scenario: Calculate Inventory at Risk When DNSA Flag is Active */
-- Calculate sum of financial_qty where dnsa_flag is 'Y'
SELECT SUM(financial_qty) AS inventory_at_risk
FROM agilisium_playground.purgo_playground.f_inv_movmnt
WHERE dnsa_flag = "Y";

/* Test Scenario: Percentage of Inventory at Risk Calculation */

-- Fetch total_inventory from a defined source
-- ASSUMPTION: total_inventory is set for this test
SELECT CASE 
WHEN defined_total_inventory IS NOT NULL THEN 
      (SUM(financial_qty) OVER (PARTITION BY dnsa_flag) / defined_total_inventory) * 100 AS percentage_of_inventory_at_risk
ELSE
      "Total inventory not provided. Calculation cannot proceed."
END 
FROM agilisium_playground.purgo_playground.f_inv_movmnt;

/* Handling undefined total inventory with error message */
-- Attempt to calculate without total_inventory
-- Expected Outcome: Error message as defined in the scenario

/* Test NULL Handling in the f_inv_movmnt Table */
-- Validate that NULL values in financial_qty are handled
SELECT 
    COUNT(*) AS null_count 
FROM 
    agilisium_playground.purgo_playground.f_inv_movmnt 
WHERE 
    financial_qty IS NULL;

/* Test Complete Pipeline for Real-time Access */
-- Test for current data reflection
-- Assert data fetched reflect updates

/* Data Schema Validation */
-- Verify schema of the f_inv_movmnt table
SHOW TABLE EXTENDED IN agilisium_playground.purgo_playground LIKE 'f_inv_movmnt';

/* PySpark Unit Test Example */