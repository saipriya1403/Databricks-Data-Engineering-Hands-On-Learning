# Day 19 – Auto Loader Structured Streaming with foreachBatch

## Objective

Learn how to use **Structured Streaming** with **foreachBatch** to process streaming data in batches.

## Flow

```text
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
Data Quality Validation
   ↓
Silver Table + Quarantine Table
```

## Input

Volume path:

```text
/Volumes/dataengineering/default/day19_customer_files/
```

Input file:

```text
customers_streaming.csv
```

## Input Data

The file contains customer records with:

* CustomerId
* CustomerName
* City
* Age
* UpdatedAt

Some records contain data-quality issues.

## Bronze

Auto Loader reads the CSV files and loads them into:

```text
bronze.day19_customers
```

The Bronze table contains the raw streaming records.

## Structured Streaming

The Bronze Delta table is used as a streaming source:

```python
silver_stream_df = (
    spark.readStream
    .table("bronze.day19_customers")
)
```

This creates a streaming DataFrame.

## foreachBatch

`foreachBatch` allows us to process each micro-batch using normal DataFrame operations.

```python
silver_query = (
    silver_stream_df
    .writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", silver_checkpoint)
    .trigger(availableNow=True)
    .start()
)

silver_query.awaitTermination()
```

## Data Quality Rules

The simplified validation checks:

1. CustomerId should not be null.
2. CustomerName should not be null.
3. Age should be between 0 and 100.

Records that pass the validation are written to:

```text
silver.day19_customers
```

Records that fail the validation are written to:

```text
silver.day19_customer_quarantine
```

## Result

Input records:

```text
6
```

Valid records:

```text
4
```

Invalid records:

```text
2
```

### Silver

Valid CustomerIds:

```text
701
702
702
705
```

### Quarantine

Invalid CustomerIds:

```text
703
704
```

## Key Concepts Learned

* Auto Loader
* Structured Streaming
* Delta table as a streaming source
* `readStream`
* `writeStream`
* `foreachBatch`
* Batch DataFrame processing
* Data quality validation
* Quarantine table
* Streaming checkpoints
* `availableNow=True`

## What I Learned

`foreachBatch` allows us to apply normal batch DataFrame logic to every streaming micro-batch.

This is useful when we need to perform custom processing such as validation and writing data to different tables.

## Day 19 Architecture

```text
Customer CSV
     ↓
Auto Loader
     ↓
Bronze
     ↓
Structured Streaming
     ↓
foreachBatch
     ↓
 ┌───────────────┐
 │ Data Quality  │
 └───────┬───────┘
         ↓
   ┌───────────────┐
   │               │
   ↓               ↓
 Silver       Quarantine
```
