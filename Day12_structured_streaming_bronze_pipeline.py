# Databricks notebook source
# MAGIC %md
# MAGIC ### Create Silver Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.day12_customers (
# MAGIC         CustomerId INT,
# MAGIC         CustomerName STRING,
# MAGIC         City STRING,
# MAGIC         Age INT,
# MAGIC         UpdatedAt TIMESTAMP
# MAGIC         )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Schema and Create Auto Loader Stream

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

customer_schema = StructType([
    StructField("CustomerId", IntegerType(), True),
        StructField("CustomerName", StringType(), True),
            StructField("City", StringType(), True),
                StructField("Age", IntegerType(), True),
                    StructField("UpdatedAt", StringType(), True)
                    ])

# COMMAND ----------

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


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create process_batch() for Bronze

# COMMAND ----------

def process_batch(batch_df, batch_id):

        batch_df.write \
                .mode("append") \
                        .saveAsTable("bronze.day12_customers")

# COMMAND ----------

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


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze.day12_customers
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create process_silver()

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable
def process_silver(batch_df, batch_id):
    batch_df = batch_df.withColumn(
                "UpdatedAt",
                        to_timestamp(col("UpdatedAt"))
                            )

    window_spec = (
                   Window
                    .partitionBy("CustomerId")
                    .orderBy(col("UpdatedAt").desc())
                    )
    latest_batch = (
                batch_df
                .withColumn("row_num", row_number().over(window_spec))
                .filter(col("row_num") == 1)
                .drop("row_num")
                .drop("_rescued_data")
                   )
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
    
     
  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start the Silver foreachBatch() Stream

# COMMAND ----------

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


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day12_customers
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the Silver Table
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day12_customers
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Processing Tes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze.day12_customers
# MAGIC WHERE CustomerId = 201
# MAGIC ORDER BY UpdatedAt;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Update Test
# MAGIC
# MAGIC **Incremental Update **Test**

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze.day12_customers
# MAGIC VALUES (
# MAGIC     201,
# MAGIC         'Arun Kumar Updated',
# MAGIC             'Chennai',
# MAGIC                 32,
# MAGIC                     '2026-09-21 09:00:00',
# MAGIC                         NULL
# MAGIC                         );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze.day12_customers
# MAGIC (CustomerId, CustomerName, City, Age, UpdatedAt)
# MAGIC VALUES (
# MAGIC     201,
# MAGIC         'Arun Kumar Updated',
# MAGIC             'Chennai',
# MAGIC                 32,
# MAGIC                     '2026-09-21 09:00:00'
# MAGIC                     );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze.day12_customers
# MAGIC WHERE CustomerId = 201
# MAGIC ORDER BY UpdatedAt;

# COMMAND ----------

batch_df = spark.table("bronze.day12_customers")

process_silver(batch_df, 0)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day12_customers
# MAGIC WHERE CustomerId = 201;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final Verification

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day12_customers
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day12_customers
# MAGIC WHERE CustomerId = 201;