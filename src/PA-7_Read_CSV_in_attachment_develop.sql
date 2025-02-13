-- Creating Delta Table in Unity Catalog Schema "purgo_playground"
CREATE TABLE IF NOT EXISTS purgo_playground.sales_data (
    country_cd STRING,
    product_id STRING,
    qty_sold INT,
    sales_date DATE
) USING DELTA
PARTITIONED BY (country_cd)
COMMENT 'Sales data table for storing product sales information'
TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true', 'delta.autoOptimize.optimizeWrite' = 'true');

-- Reading the CSV file and writing to Delta Table: Changed from COPY INTO to the appropriate format
COPY INTO purgo_playground.sales_data
FROM '/FileStore/tables/sample_sales_data.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true');

-- Verifying total sales per product
SELECT product_id, SUM(qty_sold) AS total_qty_sold
FROM purgo_playground.sales_data
GROUP BY product_id
ORDER BY product_id;

-- Merging new data into the existing Delta table
MERGE INTO purgo_playground.sales_data AS target
USING (SELECT 'US' AS country_cd, 'P1001' AS product_id, 70 AS qty_sold, '2024-01-15' AS sales_date) source
ON target.country_cd = source.country_cd AND target.product_id = source.product_id
WHEN MATCHED THEN UPDATE SET target.qty_sold = source.qty_sold
WHEN NOT MATCHED THEN INSERT (country_cd, product_id, qty_sold, sales_date) VALUES (source.country_cd, source.product_id, source.qty_sold, source.sales_date);

-- Validating data quality: Ensure no NULL fields in critical columns
SELECT *
FROM purgo_playground.sales_data
WHERE country_cd IS NULL OR qty_sold IS NULL;

-- Time travel: Querying data as of a specific version
SELECT *
FROM purgo_playground.sales_data VERSION AS OF 1;

-- Optimizing Delta table: Implement Z-ordering with a valid statement
OPTIMIZE purgo_playground.sales_data
ZORDER BY (sales_date);

-- Vacuuming Delta table to clean old data files with a valid statement
VACUUM purgo_playground.sales_data RETAIN 168 HOURS; -- Retain for 7 days

