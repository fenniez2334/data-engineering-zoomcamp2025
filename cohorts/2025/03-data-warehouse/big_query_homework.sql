-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `central-beach-447906-q6.zoomcamp.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://central-beach-447906-q6-bucket/yellow_tripdata_2024-*.parquet']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE central-beach-447906-q6.zoomcamp.yellow_tripdata_non_partitoned AS
SELECT * FROM central-beach-447906-q6.zoomcamp.external_yellow_tripdata;

-- Create a non partitioned materialized table from external table
CREATE MATERIALIZED VIEW central-beach-447906-q6.zoomcamp.yellow_tripdata_materialized AS
SELECT * FROM central-beach-447906-q6.zoomcamp.yellow_tripdata_non_partitoned;

-- Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
SELECT COUNT(DISTINCT PULocationID) as num_of_PULocation
FROM central-beach-447906-q6.zoomcamp.external_yellow_tripdata;

SELECT COUNT(DISTINCT PULocationID) as num_of_PULocation
FROM central-beach-447906-q6.zoomcamp.yellow_tripdata_materialized;


-- Write a query to retrieve the PULocationID form the table (not the external table) in BigQuery.
-- 156MB
SELECT PULocationID
FROM central-beach-447906-q6.zoomcamp.yellow_tripdata_non_partitoned;
-- 311 MB
SELECT PULocationID, DOLocationID
FROM central-beach-447906-q6.zoomcamp.yellow_tripdata_non_partitoned;


-- How many records have a fare_amount of 0?
SELECT fare_amount
FROM central-beach-447906-q6.zoomcamp.yellow_tripdata_non_partitoned
WHERE fare_amount = 0;


-- Creating a partition and cluster table
CREATE OR REPLACE TABLE central-beach-447906-q6.zoomcamp.yellow_tripdata_partitoned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM central-beach-447906-q6.zoomcamp.yellow_tripdata_non_partitoned;

-- Write a query to retrieve the distinct VendorIDs between tpep_dropoff_timedate 03/01/2024 and 03/15/2024 (inclusive)
-- 311MB
SELECT DISTINCT VendorID
FROM central-beach-447906-q6.zoomcamp.yellow_tripdata_materialized
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
-- 27MB
SELECT DISTINCT VendorID
FROM central-beach-447906-q6.zoomcamp.yellow_tripdata_partitoned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-15';


-- Write a `SELECT count(*)` query FROM the materialized table you created. 
SELECT COUNT(*) as count
FROM central-beach-447906-q6.zoomcamp.yellow_tripdata_materialized;