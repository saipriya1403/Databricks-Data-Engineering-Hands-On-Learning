# Databricks notebook source
# MAGIC %md
# MAGIC ### Create the Bronze table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze.day19_customers (
# MAGIC     CustomerId INT,
# MAGIC     CustomerName STRING,
# MAGIC     City STRING,
# MAGIC     Age INT,
# MAGIC     UpdatedAt TIMESTAMP
# MAGIC  )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Create Silver, Quarantine, and Metrics tables

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.day19_customers (
# MAGIC     CustomerId INT,
# MAGIC     CustomerName STRING,
# MAGIC     City STRING,
# MAGIC     Age INT,
# MAGIC     UpdatedAt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.day19_customer_quarantine (
# MAGIC         CustomerId INT,
# MAGIC         CustomerName STRING,
# MAGIC         City STRING,
# MAGIC         Age INT,
# MAGIC         UpdatedAt TIMESTAMP,
# MAGIC         ErrorReason STRING,
# MAGIC         RejectedAt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.day19_data_quality_metrics (
# MAGIC             BatchId STRING,
# MAGIC             TotalRecords INT,
# MAGIC             ValidRecords INT,
# MAGIC             InvalidRecords INT,
# MAGIC             QualityPercentage DOUBLE,
# MAGIC             ProcessedAt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define the customer schema

# COMMAND ----------

from pyspark.sql.types import (
        StructType,
        StructField,
        IntegerType,
        StringType,
        TimestampType
)

customer_schema = StructType([
                            StructField("CustomerId", IntegerType(), True),
                            StructField("CustomerName", StringType(), True),
                            StructField("City", StringType(), True),
                            StructField("Age", IntegerType(), True),
                            StructField("UpdatedAt", TimestampType(), True)
])


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Auto Loader streaming DataFrame

# COMMAND ----------

from pyspark.sql import functions as F

bronze_stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(customer_schema)
    .option(
    "cloudFiles.schemaLocation",
    "/Volumes/dataengineering/default/day19_customer_files/_schema/"
    )
    .load("/Volumes/dataengineering/default/day19_customer_files/")
                                                )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the Auto Loader stream to Bronze

# COMMAND ----------

bronze_checkpoint = "/Volumes/dataengineering/default/day19_customer_files/_checkpoint"

bronze_query = (
    bronze_stream_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", bronze_checkpoint)
    .trigger(availableNow=True)
    .toTable("bronze.day19_customers")
)

bronze_query.awaitTermination()

# COMMAND ----------

display(
        spark.read.table("bronze.day19_customers")
            .orderBy("CustomerId")
            
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the Bronze table as a streaming DataFrame

# COMMAND ----------

silver_stream_df = (
        spark.readStream
            .table("bronze.day19_customers")
            )

print("Is streaming:", silver_stream_df.isStreaming
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the simple data-quality function

# COMMAND ----------

def process_batch(batch_df, batch_id):

        print("Processing batch:", batch_id)

        valid_df = batch_df.filter(
            F.col("CustomerId").isNotNull()
            & F.col("CustomerName").isNotNull()
            & (F.col("Age") >= 0)
            & (F.col("Age") <= 100)
            )
        invalid_df = batch_df.filter(
                F.col("CustomerId").isNull()
                | F.col("CustomerName").isNull()
                | (F.col("Age") < 0)
                | (F.col("Age") > 100)
            )
        valid_df.write.mode("append").saveAsTable(
                    "silver.day19_customers"
                        )
        
        invalid_df = invalid_df.withColumn(
                    "ErrorReason",
                    F.lit("Data quality check failed")
                            ).withColumn(
                                        "RejectedAt",
                                        F.current_timestamp())
        invalid_df.write.mode("append").saveAsTable(
                    "silver.day19_customer_quarantine"
                        )

        print("Valid records:", valid_df.count())
        print("Invalid records:", invalid_df.count())
                           
                                                    

                            
                            
                            
                            
        
        

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start foreachBatch

# COMMAND ----------

silver_checkpoint = "/Volumes/dataengineering/default/day19_customer_files/_silver_checkpoint"

silver_query = (
    silver_stream_df
    .writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", silver_checkpoint)
    .trigger(availableNow=True)
    .start()
                        )

silver_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the Silver table

# COMMAND ----------

silver_df = spark.read.table("silver.day19_customers")

print("Silver records:", silver_df.count())

display(
    silver_df.orderBy("CustomerId")
    )

# COMMAND ----------

print("Silver count:", silver_df.count())

# COMMAND ----------

quarantine_df = spark.read.table("silver.day19_customer_quarantine")

print("Quarantine count:", quarantine_df.count())

display(
    quarantine_df.orderBy("CustomerId")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final verification

# COMMAND ----------

print("Silver count:", spark.read.table("silver.day19_customers").count())
print("Quarantine count:", spark.read.table("silver.day19_customer_quarantine").count())