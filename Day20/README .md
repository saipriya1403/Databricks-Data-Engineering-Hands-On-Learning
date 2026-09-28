# Day 20 – Gold Layer & Business Analytics

## Objective

Build a simple Gold layer from Silver tables and create business-ready datasets for analysis.

## Project Flow

```text
Silver Customers
       +
Silver Products
       +
Silver Orders
       ↓
     Join
       ↓
Gold Order Details
       ↓
Customer Revenue
       +
Overall KPI Summary
```

## Silver Tables

### 1. Customers

Table:

```text
silver.day20_customers
```

Columns:

```text
CustomerId
CustomerName
City
```

### 2. Products

Table:

```text
silver.day20_products
```

Columns:

```text
ProductId
ProductName
Category
Price
```

### 3. Orders

Table:

```text
silver.day20_orders
```

Columns:

```text
OrderId
CustomerId
ProductId
Quantity
OrderDate
```

## Gold Layer

### 1. Gold Order Details

Table:

```text
gold.day20_order_details
```

The orders and products were joined using `ProductId`.

Customer information was then added using `CustomerId`.

The following column was calculated:

```text
OrderAmount = Quantity × Price
```

The Gold Order Details table contains:

```text
OrderId
CustomerId
CustomerName
City
ProductId
ProductName
Category
Quantity
Price
OrderAmount
OrderDate
```

### Table Grain

One row represents **one order**.

## 2. Customer Revenue

Table:

```text
gold.day20_customer_revenue
```

The Gold Order Details table was grouped by customer.

Metrics created:

```text
TotalOrders
TotalRevenue
AverageOrderValue
```

Example results:

| Customer | Total Orders | Total Revenue | Average Order Value |
| -------- | -----------: | ------------: | ------------------: |
| Arun     |            2 |         64000 |               32000 |
| Kumar    |            2 |         66000 |               33000 |
| Priya    |            2 |         39000 |               19500 |
| Ravi     |            1 |         15000 |               15000 |
| Meena    |            1 |         60000 |               60000 |

## 3. Overall KPI Summary

Table:

```text
gold.day20_kpi_summary
```

KPIs created:

```text
TotalOrders
TotalRevenue
TotalUnitsSold
```

Expected results:

```text
Total Orders   = 8
Total Revenue  = 244000
Total Units     = 13
```

## Key Concepts Learned

* Gold layer
* Joining Silver tables
* Business-ready datasets
* Calculating `OrderAmount`
* Aggregation using `groupBy`
* Revenue calculation
* Customer-level analytics
* Overall business KPIs
* Table grain

## Gold Tables Created

```text
gold.day20_order_details
gold.day20_customer_revenue
gold.day20_kpi_summary
```

## Final Architecture

```text
Silver
  │
  ├── Customers
  ├── Products
  └── Orders
          │
          ↓
       Joins
          │
          ↓
       Gold Layer
          │
          ├── Order Details
          ├── Customer Revenue
          └── KPI Summary
```

## Conclusion

Day 20 focused on creating a simple Gold layer from Silver data.

The final Gold datasets are business-ready and can be used for customer revenue analysis and overall business KPI reporting.
