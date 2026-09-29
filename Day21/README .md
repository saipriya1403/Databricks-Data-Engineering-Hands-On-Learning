# Day 21 – Advanced Delta Lake

## Objective

Understand important Delta Lake features such as:

* Delta table history
* Delta table versions
* Time Travel
* UPDATE, DELETE and INSERT
* OPTIMIZE
* Z-Ordering concept
* VACUUM concept

The focus was on understanding how Delta Lake maintains different versions of a table and how historical data can be accessed.

## Project Flow

```text
gold.day20_order_details
          ↓
gold.day21_delta_lab
          ↓
    Delta History
          ↓
 UPDATE / DELETE / INSERT
          ↓
      Time Travel
          ↓
       OPTIMIZE
          ↓
 Z-Ordering / VACUUM Concepts
```

## Practice Table

Created a separate practice table:

```text
gold.day21_delta_lab
```

This table was created from:

```text
gold.day20_order_details
```

The separate table was used so that Delta Lake operations could be tested safely without changing the original Day 20 Gold table.

## 1. Delta Table History

Used:

```sql
DESCRIBE HISTORY gold.day21_delta_lab;
```

This showed:

* Version
* Timestamp
* Operation
* Operation parameters
* User information

The history showed different operations performed on the table.

## 2. UPDATE

Updated OrderId `1001`:

```sql
UPDATE gold.day21_delta_lab
SET OrderAmount = OrderAmount + 100
WHERE OrderId = 1001;
```

This created a new Delta table version.

## 3. Time Travel

Used Time Travel to see the previous state of OrderId `1001`.

Example:

```sql
SELECT *
FROM gold.day21_delta_lab VERSION AS OF 0
WHERE OrderId = 1001;
```

Time Travel allows us to read a previous version of a Delta table.

## 4. DELETE

Deleted OrderId `1002`:

```sql
DELETE FROM gold.day21_delta_lab
WHERE OrderId = 1002;
```

The record was no longer available in the current table.

Using Time Travel, the deleted record could still be viewed from the previous version.

Example:

```sql
SELECT *
FROM gold.day21_delta_lab VERSION AS OF 2
WHERE OrderId = 1002;
```

This demonstrated that Time Travel can be used to investigate previous table states.

## 5. INSERT

Inserted a new test order:

```text
OrderId = 1009
```

The INSERT created another Delta transaction/version.

## 6. OPTIMIZE

Used:

```sql
OPTIMIZE gold.day21_delta_lab;
```

OPTIMIZE is used for Delta table maintenance and physical file organization.

The operation was also recorded in Delta table history.

## 7. Z-Ordering

Learned the concept of Z-Ordering.

Example:

```sql
OPTIMIZE gold.day21_delta_lab
ZORDER BY (CustomerId);
```

Z-Ordering can improve data organization for queries that frequently filter on particular columns.

For this small practice project, the main goal was to understand the concept rather than perform performance testing.

## 8. VACUUM

Learned the concept of VACUUM.

VACUUM removes eligible old files that are no longer required by the current table state.

Important relationship:

```text
Time Travel
     ↓
Historical data
     ↓
Older files may be required
     ↓
VACUUM
     ↓
Old files can be removed
```

Therefore, Time Travel should not be treated as an unlimited backup mechanism.

## Delta History Example

During the exercises, the table history included operations such as:

```text
CREATE
UPDATE
OPTIMIZE
DELETE
OPTIMIZE
INSERT
```

The exact version numbers depend on the operations performed.

## Key Concepts Learned

* Delta Lake maintains transaction history.
* Each table change can create a new version.
* `DESCRIBE HISTORY` shows table changes.
* Time Travel can read historical table states.
* UPDATE, DELETE and INSERT create Delta transactions.
* `OPTIMIZE` improves physical file organization.
* Z-Ordering helps organize data for suitable filtering patterns.
* VACUUM removes eligible obsolete files.
* Time Travel and VACUUM are related because historical states may depend on older files.

## Final Mental Model

```text
              DELTA LAKE
                  │
        ┌─────────┴─────────┐
        ↓                   ↓
   Transaction            History
        │                   │
   UPDATE/DELETE/       Versions
     INSERT                 │
        │                   ↓
        └──────────→ Time Travel
                         │
                         ↓
                  Historical State

                         +

                  Maintenance
                   /        \
                  ↓          ↓
              OPTIMIZE     VACUUM
```

## Conclusion

Day 21 focused on understanding Delta Lake beyond basic table creation.

The main learning was how Delta Lake maintains **versioned table states**, how `DESCRIBE HISTORY` helps investigate changes, and how **Time Travel** can be used to view historical data.

We also learned the basic purpose of `OPTIMIZE`, Z-Ordering and VACUUM.
