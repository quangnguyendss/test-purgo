/*
Creating the sample_sales_data table in the purgo_playground schema.
*/
CREATE TABLE purgo_playground.sample_sales_data (
  country_cd STRING,
  product_id STRING,
  qty_sold INT,
  sales_date DATE
);

/*
Loading data into the sample_sales_data table from the provided CSV file.
*/
COPY INTO purgo_playground.sample_sales_data
FROM "dbfs:/FileStore/tables/sample_sales_data.csv"
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true');


-- Adding some rows with NULLs and special characters for testing purposes
INSERT INTO purgo_playground.sample_sales_data (country_cd, product_id, qty_sold, sales_date) VALUES (NULL, 'P1005', 10, '2024-01-24');
INSERT INTO purgo_playground.sample_sales_data (country_cd, product_id, qty_sold, sales_date) VALUES ('JP', NULL, 15, '2024-01-25');
INSERT INTO purgo_playground.sample_sales_data (country_cd, product_id, qty_sold, sales_date) VALUES ('FR', 'P1006', NULL, '2024-01-26');
INSERT INTO purgo_playground.sample_sales_data (country_cd, product_id, qty_sold, sales_date) VALUES ('DE', 'P1007', 20, NULL);

INSERT INTO purgo_playground.sample_sales_data (country_cd, product_id, qty_sold, sales_date) VALUES ('JP', 'P1008', 10, '2024-01-24');
INSERT INTO purgo_playground.sample_sales_data (country_cd, product_id, qty_sold, sales_date) VALUES ('FR', 'P1009', 15, '2024-01-25');
INSERT INTO purgo_playground.sample_sales_data (country_cd, product_id, qty_sold, sales_date) VALUES ('DE', 'P1010', NULL, '2024-01-26');