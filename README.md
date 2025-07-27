# Celebal_Week8_Assignment
Loaded nested JSON data into DBFS using PySpark in Databricks, flattened complex structures, and saved the result as an external Parquet table. This process enables efficient querying and storage of semi-structured data in a columnar format for analytics.

--"Load NYC taxi data to DataLake/Blob_Storage/DataBricks and extract the data through dataframe in the notebook. Perform Following Queries using PySpark. Query 1. - Add a column named as ""Revenue"" into dataframe which is the sum of the below columns 'Fare_amount','Extra','MTA_tax','Improvement_surcharge','Tip_amount','Tolls_amount','Total_amount' Query 2. - Increasing count of total passengers in New York City by area Query 3. - Realtime Average fare/total earning amount earned by 2 vendors Query 4. - Moving Count of payments made by each payment mode Query 5. - Highest two gaining vendor's on a particular date with no of passenger and total distance by cab Query 6. - Most no of passenger between a route of two location. Query 7. - Get top pickup locations with most passengers in last 5/10 seconds."--

# NYC Taxi Data Analysis using PySpark and Azure DataLake

This project demonstrates how to ingest NYC Taxi data into Azure DataLake/Blob Storage and process it using Azure Databricks and PySpark. The notebook performs key data analytics operations such as revenue calculation, passenger trends, vendor earnings, and more.

## Tools & Technologies
- Azure DataLake / Blob Storage
- Azure Databricks
- Apache Spark (PySpark)
- NYC Taxi Trip Record Data

## Queries Covered
1. Add "Revenue" column based on fare components
2. Count of total passengers by area
3. Realtime average fare and earnings by vendor
4. Moving count of payments per mode
5. Top 2 earning vendors on a given date
6. Most passengers between a route
7. Top pickup locations in last 5/10 seconds

--1. Load any dataset into DBFS 2. Flatten JSON fields 3. Write flattened file as external parquet table--

# JSON Flattening and Parquet External Table in Databricks

This project demonstrates how to load a nested JSON dataset into DBFS (Databricks File System), flatten complex fields using PySpark, and write the result as an external Parquet table.

## Workflow
1. Load nested JSON into DBFS
2. Flatten the JSON structure
3. Save as external Parquet table

## Tools & Technologies
- Azure Databricks
- PySpark
- Databricks File System (DBFS)

