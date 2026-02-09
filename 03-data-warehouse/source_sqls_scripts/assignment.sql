--NOTE: assignment says "For this homework we will be using the Yellow Taxi Trip Records for January 2024 - June 2024 (not the entire year of data)."

/*
--create external table
CREATE OR REPLACE EXTERNAL TABLE `dataengineeringzoomcamp-485416.nytaxi.external_table_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de-zoomcamp-pvg-11205813/yellow_tripdata_2024-*.parquet']
);


--create non partitioned table
CREATE OR REPLACE TABLE `dataengineeringzoomcamp-485416.nytaxi.nonpartitioned_table_yellow_tripdata`
AS SELECT * FROM `dataengineeringzoomcamp-485416.nytaxi.external_table_yellow_tripdata`;




--records in table for relevant data for 2024 (also see note above)
SELECT count(*) 
FROM `dataengineeringzoomcamp-485416.nytaxi.external_table_yellow_tripdata`;

*/

/*
SELECT COUNT(DISTINCT(PULocationID)) 
FROM `dataengineeringzoomcamp-485416.nytaxi.external_table_yellow_tripdata`;
*/


/*
SELECT COUNT(DISTINCT(PULocationID)) 
FROM `dataengineeringzoomcamp-485416.nytaxi.nonpartitioned_table_yellow_tripdata`;
*/

/*
SELECT PULocationID, DOLocationID
`dataengineeringzoomcamp-485416.nytaxi.nonpartitioned_table_yellow_tripdata`
*/
/*
select COUNT(*) 
FROM `dataengineeringzoomcamp-485416.nytaxi.nonpartitioned_table_yellow_tripdata`
where fare_amount = 0;
*/

/*
CREATE OR REPLACE TABLE `dataengineeringzoomcamp-485416.nytaxi.partitioned_table_yellow_tripdata`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS (
  SELECT * FROM `dataengineeringzoomcamp-485416.nytaxi.external_table_yellow_tripdata`
);
*/

/*
SELECT DISTINCT(VendorID) 
FROM  `dataengineeringzoomcamp-485416.nytaxi.nonpartitioned_table_yellow_tripdata`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
*/

/*
SELECT DISTINCT(VendorID) 
FROM `dataengineeringzoomcamp-485416.nytaxi.partitioned_table_yellow_tripdata`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
*/

/*
SELECT COUNT(*) 
FROM `dataengineeringzoomcamp-485416.nytaxi.partitioned_table_yellow_tripdata`;
*/
