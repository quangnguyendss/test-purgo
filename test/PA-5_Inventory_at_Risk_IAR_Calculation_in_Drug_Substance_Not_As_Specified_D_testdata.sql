-- Creating a Databricks SQL test table using a simplified version of the `f_inv_movmnt` schema
CREATE OR REPLACE TABLE agilisium_playground.purgo_playground.f_inv_movmnt (
    id BIGINT,
    dnsa_flag STRING,
    financial_qty DOUBLE,
    timestamp_event TIMESTAMP
);

-- Inserting diverse test records 
INSERT INTO agilisium_playground.purgo_playground.f_inv_movmnt VALUES
-- Happy Path Test Data (valid scenarios)
(1, "Y", 150.0, TIMESTAMP('2024-03-21T00:00:00.000+0000')),
(2, "N", 200.0, TIMESTAMP('2024-03-21T01:00:00.000+0000')),
(3, "Y", 300.0, TIMESTAMP('2024-03-21T02:00:00.000+0000')),

-- Edge Case: Maximum value of DOUBLE
(4, "Y", 1.79769e+308, TIMESTAMP('2024-03-21T03:00:00.000+0000')),

-- Edge Case: Minimum value of DOUBLE 
(5, "N", -1.79769e+308, TIMESTAMP('2024-03-21T04:00:00.000+0000')),

-- Error Case: Out-of-range financial_qty (negative for no DNSA flag)
(6, "N", -50.0, TIMESTAMP('2024-03-21T05:00:00.000+0000')),

-- NULL Handling: NULL financial_qty and NULL flag
(7, "Y", NULL, TIMESTAMP('2024-03-21T06:00:00.000+0000')),
(8, NULL, 120.0, TIMESTAMP('2024-03-21T07:00:00.000+0000')),

-- Special Characters and multi-byte characters in dnsa_flag
(9, "Y❤️", 180.0, TIMESTAMP('2024-03-21T08:00:00.000+0000')),
(10, "N$", 190.0, TIMESTAMP('2024-03-21T09:00:00.000+0000')),

-- Additional Happy Path Data
(11, "Y", 100.0, TIMESTAMP('2024-03-21T10:00:00.000+0000')),
(12, "N", 250.0, TIMESTAMP('2024-03-21T11:00:00.000+0000')),
(13, "Y", 350.0, TIMESTAMP('2024-03-21T12:00:00.000+0000')),

-- Valid but unexpected combination: Empty string as flag
(14, "", 400.0, TIMESTAMP('2024-03-21T13:00:00.000+0000')),

-- More NULL handling
(15, NULL, NULL, TIMESTAMP('2024-03-21T14:00:00.000+0000')),

-- Edge Case: Zero financial_qty
(16, "Y", 0.0, TIMESTAMP('2024-03-21T15:00:00.000+0000')),

-- Edge Case: Large timestamp values
(17, "Y", 500.0, TIMESTAMP('9999-12-31T23:59:59.999+0000')),
(18, "N", 600.0, TIMESTAMP('0001-01-01T00:00:00.000+0000')),

-- Additional Special Characters
(19, "!Y@", 212.0, TIMESTAMP('2024-03-21T16:00:00.000+0000')),
(20, "#N%", 312.0, TIMESTAMP('2024-03-21T17:00:00.000+0000')),

-- Valid scenario with potential edge case financial_qty
(21, "Y", 1.0, TIMESTAMP('2024-03-21T18:00:00.000+0000')),

-- Valid scenario with zero value for non-flagged entry
(22, "N", 0.0, TIMESTAMP('2024-03-21T19:00:00.000+0000')),

-- Testing large inventory movements
(23, "Y", 9876543210.0, TIMESTAMP('2024-03-21T20:00:00.000+0000')),

-- Testing negative inventory movement when flagged
(24, "Y", -500.0, TIMESTAMP('2024-03-21T21:00:00.000+0000')),

-- Valid multi-byte character scenario
(25, "你", 720.0, TIMESTAMP('2024-03-21T22:00:00.000+0000')),

-- Valid scenario with typical values
(26, "Y", 815.0, TIMESTAMP('2024-03-21T23:00:00.000+0000')),

-- Simulate an extremely large and extremely small number scenario for testing
(27, "N", 1e308, TIMESTAMP('2024-03-22T00:00:00.000+0000')),
(28, "Y", -1e308, TIMESTAMP('2024-03-22T01:00:00.000+0000'));