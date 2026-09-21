Day 13 – Auto Loader → Bronze → Silver Conditional MERGE
Objective
Build a complete incremental data pipeline using:

Auto Loader

Structured Streaming

Bronze Delta table

readStream

foreachBatch

Micro-batch deduplication

Conditional Delta MERGE

Silver Delta table

The main goal is to make sure an older record cannot overwrite a newer record in the Silver table.

Input Data
Reused the existing CSV files from:


/Volumes/dataengineering/default/day11_customer_files/
Additional test files were added:


customers_07.csv
customers_08.csv
Pipeline Flow

CSV Files
    ↓
Auto Loader
    ↓
Bronze Table
    ↓
Structured Streaming
    ↓
foreachBatch
    ↓
Latest Record per CustomerId
    ↓
Conditional MERGE
    ↓
Silver Table
Bronze Table

bronze.day13_customers
Bronze stores the incoming records and can contain multiple versions of the same customer.

Silver Table

silver.day13_customers
Silver contains the latest valid state of each customer.

Important Logic
1. Micro-batch Deduplication
Used a Window function:

Python
Window.partitionBy("CustomerId").orderBy(col("UpdatedAt").desc())
Then used:

Python

row_number()
to keep only the latest record for each CustomerId within the micro-batch.

2. Conditional MERGE
The Silver MERGE updates an existing customer only when:


source.UpdatedAt > target.UpdatedAt
Therefore:


Newer record → UPDATE
Older record → Do not update
New Customer → INSERT
Day 13 Test Cases
Test 1 – Existing Customer Update
Customer 201 received a newer record:


2026-09-21 09:00:00
The newer record updates the existing Silver record.

Test 2 – New Customer
Customer 208 was not previously present.

The record was inserted into Silver.

Test 3 – Older Record
Customer 201 later received an older record:


2026-09-19 08:00:00
Because the Silver table already contained a newer timestamp, the older record did not overwrite the existing Silver record.

Key Concept Learned
Day 13 introduced conditional Delta MERGE.

The combination of:


Micro-batch Deduplication
+
Conditional MERGE
protects the Silver table from incorrect updates caused by older records.

Day 13 Learning

Day 10 → Auto Loader

Day 11 → Auto Loader → Bronze → Deduplication → MERGE → Silver

Day 12 → Structured Streaming → AvailableNow → foreachBatch

Day 13 → Auto Loader → Bronze → readStream
          → foreachBatch → Deduplication
          → Conditional MERGE → Silver
Final Outcome
Successfully built an incremental Bronze-to-Silver pipeline that:

Detects new CSV files using Auto Loader

Loads records into Bronze

Processes Bronze using Structured Streaming

Uses foreachBatch for micro-batch processing

Keeps the latest record per customer

Inserts new customers

Updates existing customers only when the incoming record is newer

Prevents older records from overwriting newer Silver data






