# Databricks notebook source
# MAGIC %md
# MAGIC ### Create the Day 13 Silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.day13_customers (
# MAGIC         CustomerId INT,
# MAGIC         CustomerName STRING,
# MAGIC         City STRING,
# MAGIC         Age INT,
# MAGIC         UpdatedAt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure Auto Loader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

customer_schema = StructType([
    StructField("CustomerId", IntegerType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("UpdatedAt", StringType(), True)
])

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
# MAGIC ### Load the Auto Loader data into Bronze
# MAGIC

# COMMAND ----------

query = (
        bronze_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option(
                "checkpointLocation",
                "/Volumes/dataengineering/default/day11_customer_files/_day13_bronze_checkpoints/"
                )
                .trigger(availableNow=True)
                .toTable("bronze.day13_customers")
)

query.awaitTermination()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Bronze as a Streaming DataFrame

# COMMAND ----------

silver_stream = (
        spark.readStream
            .table("bronze.day13_customers")
            )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Silver processing function

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

def process_silver(batch_df, batch_id):

        print(f"Processing Silver batch: {batch_id}")
        # Convert UpdatedAt to timestamp
        batch_df = batch_df.withColumn(
                    "UpdatedAt",
                            to_timestamp(col("UpdatedAt"))
                                )
         # Keep only the latest record for each CustomerId
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
                        )
        # Get Spark session from the micro-batch DataFrame
        silver_table = DeltaTable.forName(
                    batch_df.sparkSession,
                            "silver.day13_customers"
        )
        # MERGE latest records into Silver
        (
                    silver_table.alias("target")
                    .merge(
                           latest_batch.alias("source"),
                          "target.CustomerId = source.CustomerId")
                    .whenMatchedUpdate(
                                    condition="source.UpdatedAt > target.UpdatedAt",
                                                set={
                                                    "CustomerName": "source.CustomerName",
                                                    "City": "source.City",
                                                    "Age": "source.Age",
                                                    "UpdatedAt": "source.UpdatedAt"
                                                })
                    # Insert new customers
                            .whenNotMatchedInsert(
                                        values={
                                                "CustomerId": "source.CustomerId",
                                                "CustomerName": "source.CustomerName",
                                                "City": "source.City",
                                                "Age": "source.Age",
                                                "UpdatedAt": "source.UpdatedAt"
                                                                                                                                    })
                            .execute()
                                )
                                                                                                                                            
                            
        
      
                        
                                            
                                
                                                           
                                                              
                                                                                            
                                                                                             

                                                                                                                                                
                            
        
             
            

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start the Silver Streaming Pipeline

# COMMAND ----------

silver_query = (
        silver_stream.writeStream
        .foreachBatch(process_silver)
        .option(
                "checkpointLocation",
                "/Volumes/dataengineering/default/day11_customer_files/_day13_silver_checkpoints/"
                )
                .trigger(availableNow=True)
                .start()
)

silver_query.awaitTermination()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the Silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day13_customers
# MAGIC ORDER BY CustomerId, UpdatedAt;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day13_customers
# MAGIC WHERE CustomerId = 201;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Auto Loader again

# COMMAND ----------

query = (
        bronze_stream.writeStream
            .format("delta")
                .outputMode("append")
                    .option(
                            "checkpointLocation",
                                    "/Volumes/dataengineering/default/day11_customer_files/_day13_bronze_checkpoints/"
                                        )
                                            .trigger(availableNow=True)
                                                .toTable("bronze.day13_customers")
                                                )

query.awaitTermination()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze.day13_customers
# MAGIC WHERE CustomerId IN (201, 208)
# MAGIC ORDER BY CustomerId, UpdatedAt;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Process the new Bronze records through Silver

# COMMAND ----------

silver_query = (
        silver_stream.writeStream
        .foreachBatch(process_silver)
        .option(
                "checkpointLocation",
                "/Volumes/dataengineering/default/day11_customer_files/_day13_silver_checkpoints/"
                )
                .trigger(availableNow=True)
                .start()
)

silver_query.awaitTermination()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day13_customers
# MAGIC WHERE CustomerId IN (201, 208)
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load the older record into Bronze

# COMMAND ----------

query = (
        bronze_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option(
                "checkpointLocation",
                "/Volumes/dataengineering/default/day11_customer_files/_day13_bronze_checkpoints/"
                )
                .trigger(availableNow=True)
                .toTable("bronze.day13_customers")
)

query.awaitTermination()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze.day13_customers
# MAGIC WHERE CustomerId = 201
# MAGIC ORDER BY UpdatedAt;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Process the older record through Silver

# COMMAND ----------

silver_query = (
        silver_stream.writeStream
        .foreachBatch(process_silver)
        .option(
                "checkpointLocation",
                "/Volumes/dataengineering/default/day11_customer_files/_day13_silver_checkpoints/"
                )
                .trigger(availableNow=True)
                .start()
)

silver_query.awaitTermination()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day13_customers
# MAGIC WHERE CustomerId = 201;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final Day 13 verification

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day13_customers
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day13_customers
# MAGIC WHERE CustomerId IN (201, 208)
# MAGIC ORDER BY CustomerId;
