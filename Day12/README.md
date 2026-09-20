Day 12 – Structured Streaming with foreachBatch
Objective
Implemented a Structured Streaming pipeline using Databricks Auto Loader and foreachBatch().

The pipeline processes customer data from the existing Day 11 input files, writes the streaming data to Bronze, and then processes the data into Silver using Window Functions and Delta MERGE.

Technologies Used
Databricks

PySpark

Structured Streaming

Auto Loader

foreachBatch()

Delta Lake

Window Functions

Unity Catalog

Input Data
Used the existing Day 11 customer files:


/Volumes/dataengineering/default/day11_customer_files/
The customer data contains:

CustomerId

CustomerName

City

Age

UpdatedAt

Step 1 – Create Silver Table
Created the Silver table:


silver.day12_customers
Schema:

CustomerId – INT

CustomerName – STRING

City – STRING

Age – INT

UpdatedAt – TIMESTAMP

Step 2 – Define Customer Schema
Created a predefined schema for the streaming data.

Python

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

customer_schema = StructType([
    StructField("CustomerId", IntegerType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("UpdatedAt", StringType(), True)
])
Step 3 – Create Auto Loader Stream
Used Databricks Auto Loader to read the customer CSV files incrementally.

Python

bronze_stream = (
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
Step 4 – Use foreachBatch() for Bronze
Created a process_batch() function to process each streaming batch.

Python

def process_batch(batch_df, batch_id):

    batch_df.write \
        .mode("append") \
        .saveAsTable("bronze.day12_customers")
Started the streaming query using foreachBatch().

Python

query = (
    bronze_stream.writeStream
    .foreachBatch(process_batch)
    .option(
        "checkpointLocation",
        "/Volumes/dataengineering/default/day11_customer_files/_day12_checkpoints/"
    )
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
Step 5 – Bronze Table
The streaming data was loaded into:


bronze.day12_customers
Verified the Bronze data using:

SQL

SELECT *
FROM bronze.day12_customers
ORDER BY CustomerId;
Step 6 – Create process_silver()
Created a process_silver() function to process each batch before loading it into Silver.

First converted UpdatedAt from STRING to TIMESTAMP.

Python

batch_df = batch_df.withColumn(
    "UpdatedAt",
    to_timestamp(col("UpdatedAt"))
)
Step 7 – Find Latest Customer Record
Used a Window Function with row_number() to identify the latest record for each CustomerId.

Python

window_spec = (
    Window
    .partitionBy("CustomerId")
    .orderBy(col("UpdatedAt").desc())
)
Kept only the latest record:

Python

latest_batch = (
    batch_df
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
    .drop("_rescued_data")
)
Step 8 – Delta MERGE into Silver
Used Delta Lake MERGE to update existing customers and insert new customers.

Python

silver_table = DeltaTable.forName(
    spark,
    "silver.day12_customers"
)

(
    silver_table.alias("target")
    .merge(
        latest_batch.alias("source"),
        "target.CustomerId = source.CustomerId"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
Step 9 – Silver foreachBatch()
Used foreachBatch() again to call the process_silver() function.

Python

query = (
    bronze_stream.writeStream
    .foreachBatch(process_silver)
    .option(
        "checkpointLocation",
        "/Volumes/dataengineering/default/day11_customer_files/_day12_silver_checkpoints/"
    )
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
Step 10 – Incremental Update Test
Tested an updated customer record for CustomerId 201.

The newer record contained:
CustomerId: 201
CustomerName: Arun Kumar Updated
City: Chennai
Age: 32
UpdatedAt: 2026-09-21 09:00:00
The process_silver() function identified the newer record and MERGEd it into the Silver table.

Final Data Flow

Customer CSV Files
        ↓
Databricks Auto Loader
        ↓
Structured Streaming
        ↓
foreachBatch()
        ↓
bronze.day12_customers
        ↓
foreachBatch(process_silver)
        ↓
Convert UpdatedAt to Timestamp
        ↓
Window Function
        ↓
Latest Record per CustomerId
        ↓
Delta MERGE
        ↓
silver.day12_customers
Key Learnings
Structured Streaming

Databricks Auto Loader

foreachBatch()

Streaming checkpoints

Processing streaming batches as DataFrames

Window Functions

row_number()

Latest-record processing

Delta Lake MERGE

Updating existing records

Inserting new records

Bronze and Silver processing

Day 12 Main Concept
The main concept learned in Day 12 is:

Python


Run
.foreachBatch(process_batch)
and:

Python


Run
.foreachBatch(process_silver)
foreachBatch() allows custom processing logic to be applied to every micro-batch of a Structured Streaming job.
