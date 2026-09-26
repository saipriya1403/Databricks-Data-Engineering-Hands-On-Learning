# Day 18 – Auto Loader Data Quality and Quarantine

## Objective

Build a simple data-quality pipeline using:

**CSV → Auto Loader → Bronze → Data Quality Checks → Silver + Quarantine**

## Input

Input folder:

`/Volumes/dataengineering/default/day18_customer_files/`

Input file:

`customers_quality.csv`

## Data Quality Rules

The following rules were applied:

1. CustomerId must not be null.
2. CustomerName must not be null or empty.
3. Age must be between 0 and 100.
4. Duplicate CustomerId records are quarantined.

## Bronze Layer

Auto Loader reads the CSV file and loads the records into:

`bronze.day18_customers`

The Auto Loader schema and checkpoint locations were stored inside the Day 18 Volume.

## Data Quality Processing

The Bronze table was read as a batch DataFrame.

Duplicate CustomerIds were identified first.

Then each record was checked against the data-quality rules.

Records were divided into:

* Valid records
* Invalid records

## Silver Layer

Valid records were saved to:

`silver.day18_customers`

The valid records were:

* CustomerId 601
* CustomerId 605

Total valid records: **2**

## Quarantine Layer

Invalid records were saved to:

`silver.day18_customer_quarantine`

The quarantine table contains the original record, `ErrorReason`, and `RejectedAt`.

Invalid records:

* CustomerId 602 → Duplicate CustomerId
* CustomerId 602 → Duplicate CustomerId
* CustomerId 603 → CustomerName is null or empty
* CustomerId 604 → Age is invalid

Total quarantined records: **4**

## Final Result

| Layer      | Table                              | Records |
| ---------- | ---------------------------------- | ------: |
| Bronze     | `bronze.day18_customers`           |       6 |
| Silver     | `silver.day18_customers`           |       2 |
| Quarantine | `silver.day18_customer_quarantine` |       4 |

## Key Concepts Learned

* Auto Loader
* Explicit schema
* CloudFiles schema location
* Streaming ingestion
* Delta Bronze table
* Data-quality validation
* Duplicate detection
* Quarantine pattern
* Valid vs invalid records
* Silver layer processing

## Day 18 Flow

```text
customers_quality.csv
        ↓
    Auto Loader
        ↓
Bronze
        ↓
Data Quality Checks
      ↙       ↘
   Valid     Invalid
     ↓          ↓
  Silver    Quarantine
```
