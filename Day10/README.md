Day 10 – Auto Loader Incremental File Processing
Objective
In this task, I implemented Databricks Auto Loader to ingest customer CSV files incrementally into a Bronze Delta table. I also handled schema/type issues and data-quality validation by separating valid and invalid records.

Technologies Used
Databricks

PySpark

Auto Loader

Delta Lake

Unity Catalog

CSV

Input Data
Customer CSV files were uploaded to the Databricks Volume:


/Volumes/dataengineering/default/day10_customer_files/
The initial batch contained:


customer_1.csv
customer_2.csv
customer_3.csv
customer_4.csv
customer_5.csv
customer_6.csv
An additional file was later added to demonstrate incremental processing:


customer_7.csv
Step 1 – Define Schema
A predefined schema was created for the customer data:

Python

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

customer_schema = StructType([
    StructField("CustomerId", IntegerType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Age", IntegerType(), True)
])
Step 2 – Configure Auto Loader
Auto Loader was configured using the cloudFiles format.

Python

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(customer_schema)
    .option("rescuedDataColumn", "_rescued_data")
    .option(
        "cloudFiles.schemaLocation",
        "/Volumes/dataengineering/default/day10_customer_files/_schema/"
    )
    .load("/Volumes/dataengineering/default/day10_customer_files/")
)
Step 3 – Load Data into Bronze
The streaming data was written into a Bronze Delta table using availableNow=True.

Python
query = (
    df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/Volumes/dataengineering/default/day10_customer_files/_checkpoints/"
    )
    .trigger(availableNow=True)
    .toTable("bronze.day10_customers")
)
Bronze table:


bronze.day10_customers
Step 4 – Handle Data Quality Issues
The Bronze table was read as a normal batch DataFrame:

Python
bronze_df = spark.read.table("bronze.day10_customers")
Invalid records were identified based on:

Null CustomerId

Null CustomerName

Null City

Null Age

Non-null _rescued_data

Python
invalid_df = bronze_df.filter(
    col("CustomerId").isNull()
    | col("CustomerName").isNull()
    | col("City").isNull()
    | col("Age").isNull()
    | col("_rescued_data").isNotNull()
)
Step 5 – Separate Valid Records
Valid records were filtered using the opposite conditions:

Python
valid_df = bronze_df.filter(
    col("CustomerId").isNotNull()
    & col("CustomerName").isNotNull()
    & col("City").isNotNull()
    & col("Age").isNotNull()
    & col("_rescued_data").isNull()
)
Step 6 – Quarantine Invalid Records
Invalid records were stored separately:


silver.day10_customer_quarantine
This allows problematic records to be isolated instead of loading them into the clean Silver dataset.

Step 7 – Store Valid Records
Valid records were stored in:


silver.day10_customers
Step 8 – Incremental File Processing
A new file, customer_7.csv, was added to the same input Volume.

The same Auto Loader checkpoint was reused so that previously processed files were not processed again.

The new records were successfully appended to:


bronze.day10_customers
Data Quality Examples
The dataset intentionally contained some invalid values to demonstrate Auto Loader's handling:

Age = ABC → captured using _rescued_data

Blank Age → stored as NULL

Blank City → stored as NULL

Key Learning
Through this exercise, I learned how to:

Configure Databricks Auto Loader

Ingest CSV files incrementally

Use predefined schemas

Use _rescued_data for unexpected data types

Use checkpoint locations for streaming workloads

Separate valid and invalid records

Implement Bronze and Silver layers

Process newly arriving files incrementally

Project Structure

Day10/
├── Day10_AutoLoader_Incremental_File_Processing.py
├── customer_1.csv
├── customer_2.csv
├── customer_3.csv
├── customer_4.csv
├── customer_5.csv
├── customer_6.csv
├── customer_7.csv
└── README.md
