# Databricks notebook source
# MAGIC %md
# MAGIC ### Import required functions

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

customers_day8 = [
        (1, "Arjun", "Chennai", 29, "arjun@gmail.com", "2026-09-15 09:00:00"),
        (2, "Priya", "Bangalore", 26, "priya@gmail.com", "2026-09-15 09:05:00"),
        (1, "Arjun", "Bangalore", 30, "arjun@gmail.com", "2026-09-15 10:00:00"),
        (3, "Michael", "Hyderabad", 34, "michael@gmail.com", "2026-09-15 10:10:00"),
        (1, "Arjun", "Hyderabad", 31, "arjun@gmail.com", "2026-09-15 11:00:00"),
        (4, "Neha", "Coimbatore", 31, "neha@gmail.com", "2026-09-15 11:15:00"),
        (2, "Priya", "Chennai", 27, "priya@gmail.com", "2026-09-15 11:30:00"),
        (5, "Vikram", "Madurai", 38, "vikram@gmail.com", "2026-09-15 11:45:00")
]                                   

columns_day8 = [
                "CustomerId",
                "CustomerName",
                "City",
                "Age",
                "Email",
                "UpdatedAt"]
                                                           

df_day8 = spark.createDataFrame(
                                customers_day8,
                                columns_day8)
display(df_day8)                                                                   

                                      


# COMMAND ----------

# MAGIC %md
# MAGIC ###  Store the data in Bronze

# COMMAND ----------

df_day8.write.format("delta").mode("overwrite").saveAsTable(
        "bronze.day8_customer_updates"
        )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Bronze data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM bronze.day8_customer_updates
# MAGIC ORDER BY CustomerId, UpdatedAt;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find duplicate CustomerIds

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     CustomerId,
# MAGIC         COUNT(*) AS RecordCount
# MAGIC         FROM bronze.day8_customer_updates
# MAGIC         GROUP BY CustomerId
# MAGIC         HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Window Specification

# COMMAND ----------

window_spec = (
        Window
            .partitionBy("CustomerId")
                .orderBy(col("UpdatedAt").desc())
                )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply ROW_NUMBER()

# COMMAND ----------

ranked_df = (
        df_day8
            .withColumn(
                    "row_num",
                            row_number().over(window_spec)
                                )
                                )

display(ranked_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Keep Only the Latest Record

# COMMAND ----------

latest_df = (
        ranked_df
            .filter(col("row_num") == 1)
                .drop("row_num")
                )

display(latest_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Deduplicated Data to Silver

# COMMAND ----------

latest_df.write.format("delta").mode("overwrite").saveAsTable(
        "silver.day8_customers"
        )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM silver.day8_customers
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify that duplicates are removed

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     CustomerId,
# MAGIC         COUNT(*) AS RecordCount
# MAGIC         FROM silver.day8_customers
# MAGIC         GROUP BY CustomerId
# MAGIC         HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Advanced Exercise: BatchId
# MAGIC

# COMMAND ----------

customers_batch = [
        (1, "Arjun", "Chennai", 29, "arjun@gmail.com", "2026-09-15 09:00:00", 1),
        (2, "Priya", "Bangalore", 26, "priya@gmail.com", "2026-09-15 09:05:00", 1),
        (1, "Arjun", "Bangalore", 30, "arjun@gmail.com", "2026-09-15 10:00:00", 2),
        (3, "Michael", "Hyderabad", 34, "michael@gmail.com", "2026-09-15 10:10:00", 2),
        (1, "Arjun", "Hyderabad", 31, "arjun@gmail.com", "2026-09-15 11:00:00", 3),
        (1, "Arjun", "Chennai", 32, "arjun@gmail.com", "2026-09-15 11:00:00", 4)
]                           

batch_columns = [
                "CustomerId",
                "CustomerName",
                "City",
                "Age",
                "Email",
                "UpdatedAt",
                "BatchId"
]

batch_df = spark.createDataFrame(
                                customers_batch,
                                batch_columns)
                                                                

display(batch_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ###  Window using UpdatedAt + BatchId

# COMMAND ----------

window_batch_spec = (
        Window
        .partitionBy("CustomerId")
        .orderBy(
        col("UpdatedAt").desc(),
        col("BatchId").desc()))
                                    
                                    


# COMMAND ----------

ranked_batch_df = (
        batch_df
        .withColumn(
        "row_num",
        row_number().over(window_batch_spec)))
                                
                               

display(ranked_batch_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Get the Latest Batch Record

# COMMAND ----------

filtered_batch_df = (
        ranked_batch_df
            .filter(col("row_num") == 1)
                .drop("row_num")
                )

display(filtered_batch_df)
