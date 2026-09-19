Day 11 – Auto Loader Latest Record Processing and MERGE
Objective
Implemented an incremental customer data pipeline using Databricks Auto Loader and Delta Lake MERGE.

The pipeline identifies the latest record for each CustomerId using the UpdatedAt timestamp and merges the latest records into the Silver table.

Technologies Used
Databricks

PySpark

Auto Loader

Delta Lake

Unity Catalog

Window Functions

CSV

Input Files
Location:

/Volumes/dataengineering/default/day11_customer_files/

Files:

customer_updates_1.csv

customer_updates_2.csv

customer_updates_3.csv

Columns:

CustomerId

CustomerName

City

Age

UpdatedAt

Step 1 – Define Schema
Created a predefined schema for the customer data.

Python

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

customer_schema = StructType([
    StructField("CustomerId", IntegerType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("UpdatedAt", StringType(), True)
])
Step 2 – Auto Loader
Used Databricks Auto Loader to read CSV files incrementally.

Python

bronze_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(customer_schema)
    .option(
        "cloudFiles.schemaLocation",
        "/Volumes/dataengineering/default/day11_customer_files/_schema/"
    )
    .load("/Volumes/dataengineering/default/day11_customer_files/")
)
Step 3 – Write to Bronze
Loaded the data into:

bronze.day11_customers

Python


query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/Volumes/dataengineering/default/day11_customer_files/_checkpoints/"
    )
    .trigger(availableNow=True)
    .toTable("bronze.day11_customers")
)
Step 4 – Convert UpdatedAt
Read the Bronze table and converted UpdatedAt from String to Timestamp.

Python

from pyspark.sql.functions import col, to_timestamp

bronze = spark.read.table("bronze.day11_customers")

bronze = bronze.withColumn(
    "UpdatedAt",
    to_timestamp(col("UpdatedAt"))
)

display(bronze)
Step 5 – Find Latest Record
Used Window Function and row_number() to identify the latest record for each CustomerId.

Python

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = (
    Window
    .partitionBy("CustomerId")
    .orderBy(col("UpdatedAt").desc())
)

latest_customers = (
    bronze
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

display(latest_customers)
This keeps only the latest record for each CustomerId.

Step 6 – Create Silver Table
Created:

silver.day11_customers

SQL

CREATE TABLE IF NOT EXISTS silver.day11_customers (
    CustomerId INT,
    CustomerName STRING,
    City STRING,
    Age INT,
    UpdatedAt TIMESTAMP
);
Step 7 – MERGE into Silver
Used Delta Lake MERGE to update existing customers and insert new customers.

Python

from delta.tables import DeltaTable

silver_table = DeltaTable.forName(
    spark,
    "silver.day11_customers"
)

(
    silver_table.alias("target")
    .merge(
        latest_customers.alias("source"),
        "target.CustomerId = source.CustomerId"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
Step 8 – Incremental Update
Added a new CSV file:

customer_updates_3.csv

The file contained:

An updated record for CustomerId 201

A new customer with CustomerId 208

The same Auto Loader checkpoint was used to process the new file.

This demonstrated:

Existing customer update

New customer insertion

Incremental file processing

Latest-record processing

Delta MERGE

Final Data Flow
Customer CSV Files
↓
Auto Loader
↓
Bronze Table
bronze.day11_customers
↓
Convert UpdatedAt to Timestamp
↓
Window Function + row_number()
↓
Latest Record per CustomerId
↓
Delta MERGE
↓
Silver Table
silver.day11_customers

Key Learnings
Auto Loader for incremental file ingestion

Predefined schema

Streaming checkpoints

Timestamp conversion

Window Functions

row_number()

Latest-record identification

Delta Lake MERGE

Updating existing records

Inserting new records

Incremental Bronze-to-Silver processing

Project Structure
Day11/

Day11_Auto_Loader_Latest_Record_Merge.py

customer_updates_1.csv

customer_updates_2.csv

customer_updates_3.csv

README.md
