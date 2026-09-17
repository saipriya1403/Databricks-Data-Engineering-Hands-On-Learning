# Day 9 – Complete Incremental Order Pipeline

## Objective

Build a complete incremental order processing pipeline using **PySpark, Delta Lake, Data Quality checks, Window Functions, and MERGE**.

The pipeline processes incoming order updates, validates the data, keeps the latest record for each order, stores the result in the Silver layer, and then processes a new incremental batch using Delta Lake MERGE.

---

## Project Flow

```text
Incoming Order Updates
        ↓
Bronze Layer
        ↓
Data Quality Validation
        ↓
Window Function Deduplication
        ↓
Latest Order Records
        ↓
Silver Layer
        ↓
New Incremental Batch
        ↓
MERGE
        ↓
Updated Silver Orders
```

---

## Step 1 – Create Incoming Order Updates

Created sample RetailMart order updates using PySpark.

The dataset contains:

* OrderId
* CustomerId
* Amount
* Status
* UpdatedAt

Some orders appear multiple times because their status changes over time.

For example, OrderId `1001` has different statuses such as:

```text
Created
Shipped
Delivered
```

The `UpdatedAt` column is used to identify the latest version of an order.

---

## Step 2 – Bronze Layer

Stored the incoming order updates as a Delta table:

```text
bronze.day9_order_updates
```

The Bronze layer contains the raw incoming order update records before final processing.

---

## Step 3 – Data Quality Validation

Applied the following data quality rules:

* `OrderId` cannot be NULL
* `CustomerId` cannot be NULL
* `Amount` must be greater than 0
* `Status` cannot be NULL
* `UpdatedAt` cannot be NULL

Created separate DataFrames for invalid and valid records.

### Important Logic

For identifying invalid records, `OR (|)` is used because a record is invalid if **any one** of the rules fails.

For keeping valid records, `AND (&)` is used because **all rules must pass**.

---

## Step 4 – Latest Record Deduplication

Used a PySpark Window Function to identify the latest record for each `OrderId`.

The window is partitioned by:

```text
OrderId
```

and ordered by:

```text
UpdatedAt DESC
```

Used:

```text
row_number()
```

to assign a row number to each version of an order.

Only records with:

```text
row_num = 1
```

were retained.

This ensures that only the latest status of each order is stored in the Silver layer.

---

## Step 5 – Silver Layer

Stored the latest order records in:

```text
silver.day9_orders
```

At this stage, each `OrderId` has only one latest record.

Example:

```text
1001 → Delivered
1002 → Shipped
1003 → Created
```

---

## Step 6 – Incremental Batch

Created a new batch of orders representing new data arriving later.

The incremental batch contained:

* Existing orders that required updates
* A new order that needed to be inserted

Example:

```text
1001 → Delivered
1002 → Delivered
1010 → Created
```

The new batch was registered as a temporary SQL view:

```text
new_orders_day9
```

---

## Step 7 – Delta MERGE

Used Delta Lake `MERGE` to process the incremental batch.

The merge condition was:

```text
target.OrderId = source.OrderId
```

### WHEN MATCHED

Existing orders were updated with the latest:

* CustomerId
* Amount
* Status
* UpdatedAt

### WHEN NOT MATCHED

New orders were inserted into the Silver table.

Therefore, the MERGE operation performs both:

```text
UPDATE existing records
INSERT new records
```

---

## Step 8 – Final Verification

Verified the final Silver table using:

```sql
SELECT *
FROM silver.day9_orders
ORDER BY OrderId;
```

The final table contains the latest version of existing orders along with newly inserted orders.

---

## Step 9 – Delta Table History

Checked the Delta Lake history using:

```sql
DESCRIBE HISTORY silver.day9_orders;
```

This helps track the operations performed on the Delta table.

---

## Key Concepts Learned

* Incremental data processing
* Bronze and Silver data layers
* Data quality validation
* Valid vs invalid record separation
* PySpark Window Functions
* `row_number()`
* Deduplication based on latest timestamp
* Delta Lake `MERGE`
* Updating existing records
* Inserting new records
* Temporary SQL views
* Delta table history

---

## Final Architecture

```text
             RetailMart Order Updates
                       │
                       ▼
             bronze.day9_order_updates
                       │
                       ▼
              Data Quality Checks
                  │          │
                Valid      Invalid
                  │
                  ▼
           Window Function
                  │
                  ▼
          Latest Order Records
                  │
                  ▼
            silver.day9_orders
                  │
                  ▼
          Incremental New Batch
                  │
                  ▼
                MERGE
              /       \
          MATCHED   NOT MATCHED
             │           │
           UPDATE      INSERT
              \         /
               \       /
                ▼     ▼
             Updated
        silver.day9_orders
```

## Tables Created

### Bronze

```text
bronze.day9_order_updates
```

### Silver

```text
silver.day9_orders
```

---

## Technologies Used

* Python
* PySpark
* Databricks
* Delta Lake
* SQL
* Window Functions
* MERGE
* Data Quality Validation
