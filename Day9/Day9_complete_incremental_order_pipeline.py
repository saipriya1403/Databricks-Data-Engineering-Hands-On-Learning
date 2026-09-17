# Databricks notebook source
# MAGIC %md
# MAGIC ### Create Day 9 order updates

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
orders_day9 = [
        (1001, 1, 55000, "Created", "2026-09-16 09:00:00"),
        (1002, 2, 50000, "Created", "2026-09-16 09:05:00"),
        (1001, 1, 55000, "Shipped", "2026-09-16 10:00:00"),
        (1003, 3, 3000, "Created", "2026-09-16 10:10:00"),
        (1001, 1, 55000, "Delivered", "2026-09-16 11:00:00"),
        (1002, 2, 50000, "Shipped", "2026-09-16 11:30:00")
]

columns_day9 = [
               "OrderId",
               "CustomerId",
                "Amount",
                "Status",
                "UpdatedAt"
]

df_day9 = spark.createDataFrame(orders_day9, columns_day9)

display(df_day9)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Store the incoming data in Bronze

# COMMAND ----------

df_day9.write.format("delta").mode("overwrite").saveAsTable(
        "bronze.day9_order_updates"
        )


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze.day9_order_updates
# MAGIC ORDER BY OrderId, UpdatedAt;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply data quality rules
# MAGIC OrderId cannot be NULL
# MAGIC
# MAGIC CustomerId cannot be NULL
# MAGIC
# MAGIC Amount must be greater than 0
# MAGIC
# MAGIC Status cannot be NULL
# MAGIC
# MAGIC UpdatedAt cannot be NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find invalid records

# COMMAND ----------

invalid_df = df_day9.filter(
        col("OrderId").isNull()
        | col("CustomerId").isNull()
        | (col("Amount") <= 0)
        | col("Status").isNull()
        | col("UpdatedAt").isNull()
)

display(invalid_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ###  Keep only valid records

# COMMAND ----------

valid_df = df_day9.filter(
        col("OrderId").isNotNull()
        & col("CustomerId").isNotNull()
        & (col("Amount") > 0)
        & col("Status").isNotNull()
        & col("UpdatedAt").isNotNull()
)

display(valid_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Find the latest order record

# COMMAND ----------

window_spec = (
        Window
            .partitionBy("OrderId")
                .orderBy(col("UpdatedAt").desc())
                )


# COMMAND ----------

ranked_df = valid_df.withColumn(
        "row_num",
            row_number().over(window_spec)
            )

display(ranked_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Keep only the latest records

# COMMAND ----------

latest_orders = (
        ranked_df
            .filter(col("row_num") == 1)
                .drop("row_num")
                )

display(latest_orders)


# COMMAND ----------

# MAGIC %md
# MAGIC ###  Save the latest orders to Silver

# COMMAND ----------

latest_orders.write.format("delta").mode("overwrite").saveAsTable(
        "silver.day9_orders"
        )


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day9_orders
# MAGIC ORDER BY OrderId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a new incremental batch

# COMMAND ----------

new_orders_day9 = [
        (1001, 1, 55000, "Delivered", "2026-09-17 09:00:00"),
        (1002, 2, 50000, "Delivered", "2026-09-17 09:05:00"),
        (1010, 5, 12000, "Created", "2026-09-17 09:10:00")
                ]

new_df = spark.createDataFrame(
                    new_orders_day9,
                        columns_day9
                        )

display(new_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a temporary view

# COMMAND ----------

new_df.createOrReplaceTempView("new_orders_day9")

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE the incremental data

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.day9_orders AS target
# MAGIC USING new_orders_day9 AS source
# MAGIC ON target.OrderId = source.OrderId
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC       target.CustomerId = source.CustomerId,
# MAGIC       target.Amount = source.Amount,
# MAGIC       target.Status = source.Status,
# MAGIC       target.UpdatedAt = source.UpdatedAt
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC                     INSERT (
# MAGIC                     OrderId,
# MAGIC                     CustomerId,
# MAGIC                     Amount,
# MAGIC                     Status,
# MAGIC                     UpdatedAt
# MAGIC )
# MAGIC VALUES (
# MAGIC         source.OrderId,
# MAGIC         source.CustomerId,
# MAGIC         source.Amount,
# MAGIC         source.Status,
# MAGIC         source.UpdatedAt
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final verification

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day9_orders
# MAGIC ORDER BY OrderId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Delta history

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver.day9_orders;
