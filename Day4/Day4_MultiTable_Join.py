# Databricks notebook source
# MAGIC %md
# MAGIC Read your Silver Customers table

# COMMAND ----------

customers_df = spark.table("silver.customers")

display(customers_df)

# COMMAND ----------

customers_df.printSchema()
customers_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Product Data

# COMMAND ----------

products = [
        (101, "Laptop", "Electronics", 55000),
            (102, "Mobile", "Electronics", 25000),
                (103, "Headphones", "Accessories", 3000),
                    (104, "Keyboard", "Accessories", 2000),
                        (105, "Monitor", "Electronics", 12000)
                        ]

product_columns = [
"ProductId",
"ProductName",
"Category",
"Price"
]

products_df = spark.createDataFrame(
products,
product_columns
)

display(products_df)


# COMMAND ----------

products_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Products to Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;

# COMMAND ----------

products_df.write \
        .mode("overwrite") \
            .saveAsTable("silver.products")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *from silver.products ;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Orders Data

# COMMAND ----------

orders = [
        (1001, 1, 101, 1, "2026-09-01"),
        (1002, 2, 102, 2, "2026-09-01"),
        (1003, 3, 103, 1, "2026-09-02"),
        (1004, 1, 104, 2, "2026-09-02"),
        (1005, 4, 105, 1, "2026-09-03"),
        (1006, 5, 101, 1, "2026-09-03"),
        (1007, 6, 103, 2, "2026-09-04"),
        (1008, 7, 102, 1, "2026-09-04"),
        (1009, 8, 105, 2, "2026-09-05")
        ]

order_columns = [
"OrderId",
"CustomerId",
"ProductId",
"Quantity",
"OrderDate"
]

orders_df = spark.createDataFrame(
orders,
order_columns
)

display(orders_df)


# COMMAND ----------

orders_df.printSchema()
orders_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Orders to Silver

# COMMAND ----------

orders_df.write \
        .mode("overwrite") \
            .saveAsTable("silver.orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from silver.orders;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read All Three Silver Tables

# COMMAND ----------

customers_df = spark.table("silver.customers")
products_df = spark.table("silver.products")
orders_df = spark.table("silver.orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Join Orders with Customers

# COMMAND ----------

order_customer_df = orders_df.join(
        customers_df,
        orders_df.CustomerId == customers_df.CustomerId,
        "inner"
)

display(order_customer_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Join Products

# COMMAND ----------

order_details_df = order_customer_df.join(
        products_df,
        order_customer_df.ProductId == products_df.ProductId,
        "inner")
                

display(order_details_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Select Required Columns

# COMMAND ----------

order_details_df = order_details_df.select(
        orders_df.OrderId,
        customers_df.CustomerId,
        customers_df.CustomerName,
        customers_df.City,
        products_df.ProductId,
        products_df.ProductName,
        products_df.Category,
        products_df.Price,
        orders_df.Quantity,
        orders_df.OrderDate)
                                            

display(order_details_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate Order Amount

# COMMAND ----------

from pyspark.sql.functions import col

order_details_df = order_details_df.withColumn(
    "OrderAmount",
    col("Price") * col("Quantity"))
        

display(order_details_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Gold Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Gold Order Details

# COMMAND ----------

order_details_df.write \
        .mode("overwrite") \
        .saveAsTable("gold.order_details")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from gold.order_details;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate Customer Revenue

# COMMAND ----------

from pyspark.sql.functions import sum

customer_revenue_df = order_details_df.groupBy(
    "CustomerId",
    "CustomerName"
    ).agg(
    sum("OrderAmount").alias("TotalRevenue")
            )

display(customer_revenue_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Customer Revenue Gold Table

# COMMAND ----------

customer_revenue_df.write \
        .mode("overwrite") \
            .saveAsTable("gold.customer_revenue")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold.customer_revenue;
# MAGIC
