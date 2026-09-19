# Databricks notebook source
# MAGIC %md
# MAGIC ### Define the Day 11 schema

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

# MAGIC %md
# MAGIC ### Configure Auto Loader and load the CSV files into Bronze

# COMMAND ----------

bronze_df = (
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

query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
                "checkpointLocation",
                "/Volumes/dataengineering/default/day11_customer_files/_checkpoints/"
                                        )
                .trigger(availableNow=True)
                .toTable("bronze.day11_customers")
                                                )


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze.day11_customers
# MAGIC ORDER BY CustomerId, UpdatedAt;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert UpdatedAt to a timestamp

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

bronze = spark.read.table("bronze.day11_customers")

bronze = bronze.withColumn(
    "UpdatedAt",
        to_timestamp(col("UpdatedAt"))
        )

display(bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Keep only the latest record for each customer

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window_spec = (
    Window
    .partitionBy("CustomerId")
    .orderBy(col("UpdatedAt").desc())
            )

latest_customers = (
                bronze
                    .withColumn("row_num", row_number().over(window_spec))
                        .filter(col("row_num") == 1)
                            .drop("row_num")
                            )

display(latest_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.day11_customers (
# MAGIC         CustomerId INT,
# MAGIC             CustomerName STRING,
# MAGIC                 City STRING,
# MAGIC                     Age INT,
# MAGIC                         UpdatedAt TIMESTAMP
# MAGIC                         );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE the latest records into Silver

# COMMAND ----------

from delta.tables import DeltaTable

silver_table = DeltaTable.forName(
    spark,
        "silver.day11_customers"
        )

(
        silver_table.alias("target")
        .merge(
                latest_customers.alias("source"),
               "target.CustomerId = source.CustomerId"
        )
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
        .execute()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the Silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day11_customers
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test an update

# COMMAND ----------

query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
                "checkpointLocation",
                "/Volumes/dataengineering/default/day11_customer_files/_checkpoints/"
                                        )
        .trigger(availableNow=True)
        .toTable("bronze.day11_customers")
)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day11_customers
# MAGIC WHERE CustomerId IN (201, 208)
# MAGIC ORDER BY CustomerId;
