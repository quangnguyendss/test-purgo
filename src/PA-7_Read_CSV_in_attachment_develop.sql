/* Create the table in Unity Catalog */
CREATE OR REPLACE TABLE purgo_playground.sample_sales_data (
  country_cd STRING,
  product_id STRING,
  qty_sold INT,
  sales_date DATE
);

/* Load data from the provided CSV */
COPY INTO purgo_playground.sample_sales_data
FROM "dbfs:/FileStore/tables/sample_sales_data.csv"
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')
COPY_OPTIONS ('force' = 'true'); -- Use force to overwrite if the table exists


-- Additional data for testing NULLs, special, and multi-byte characters as per the test suite provided
INSERT INTO purgo_playground.sample_sales_data (country_cd, product_id, qty_sold, sales_date) VALUES ('JP', 'P1005', 10, '2024-01-24');
INSERT INTO purgo_playground.sample_sales_data (country_cd, product_id, qty_sold, sales_date) VALUES ('FR', 'P1006', 15, '2024-01-25');

INSERT INTO purgo_playground.sample_sales_data (country_cd, product_id, qty_sold, sales_date) VALUES (NULL, 'P1007', 20, '2024-01-26');
INSERT INTO purgo_playground.sample_sales_data (country_cd, product_id, qty_sold, sales_date) VALUES ('US', NULL, 25, '2024-01-27');
INSERT INTO purgo_playground.sample_sales_data (country_cd, product_id, qty_sold, sales_date) VALUES ('CA', 'P1008', NULL, '2024-01-28');
INSERT INTO purgo_playground.sample_sales_data (country_cd, product_id, qty_sold, sales_date) VALUES ('UK', 'P1009', 30, NULL);

INSERT INTO purgo_playground.sample_sales_data (country_cd, product_id, qty_sold, sales_date) VALUES ('BR', 'P1010', 35, '2024-01-29'); -- Testing multi-byte char

