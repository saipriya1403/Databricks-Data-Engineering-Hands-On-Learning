# Databricks notebook source
# MAGIC %md
# MAGIC ### Create the Bronze table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE bronze.day18_customers (
# MAGIC     CustomerId INT,
# MAGIC     CustomerName STRING,
# MAGIC     City STRING,
# MAGIC     Age INT,
# MAGIC     UpdatedAt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Silver and Quarantine tables

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE silver.day18_customers (
# MAGIC     CustomerId INT,
# MAGIC     CustomerName STRING,
# MAGIC     City STRING,
# MAGIC     Age INT,
# MAGIC     UpdatedAt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC CREATE TABLE silver.day18_customer_quarantine (
# MAGIC                         CustomerId INT,
# MAGIC                         CustomerName STRING,
# MAGIC                         City STRING,
# MAGIC                         Age INT,
# MAGIC                         UpdatedAt TIMESTAMP,
# MAGIC                         ErrorReason STRING,
# MAGIC                         RejectedAt TIMESTAMP
# MAGIC                         )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Auto Loader stream

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

bronze_stream = (
                spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", "true")
                .schema(customer_schema)
                .option(
                        "cloudFiles.schemaLocation",
                        "/Volumes/dataengineering/default/day18_customer_files/_schema/"
                        )
                       .load("/Volumes/dataengineering/default/day18_customer_files/")
                        )

print("Is streaming:", bronze_stream.isStreaming)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Load the CSV into Bronze

# COMMAND ----------

query = (
        bronze_stream.writeStream
       .format("delta")
       .outputMode("append")
       .option(
                "checkpointLocation",
                "/Volumes/dataengineering/default/day18_customer_files/_checkpoint/"
               )
       .trigger(availableNow=True)
       .toTable("bronze.day18_customers")
)

query.awaitTermination()


# COMMAND ----------

display(spark.read.table("bronze.day18_customers"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Bronze as a batch

# COMMAND ----------

bronze_df = spark.read.table("bronze.day18_customers")

display(bronze_df)

# COMMAND ----------

print("Bronze record count:", bronze_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Find duplicate CustomerIds

# COMMAND ----------

from pyspark.sql.functions import col

duplicate_ids = (
    bronze_df
   .groupBy("CustomerId")
   .count()
    .filter(col("count") > 1)
    .select("CustomerId")
)

display(duplicate_ids)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mark duplicate records

# COMMAND ----------

from pyspark.sql.functions import lit

df_flagged = (
    bronze_df
    .join(
        duplicate_ids.withColumn("IsDuplicate", lit(True)),
        on="CustomerId",
        how="left"
        )
       .fillna({"IsDuplicate": False})
)

display(df_flagged)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Apply the data-quality rules

# COMMAND ----------

from pyspark.sql.functions import col, trim, when, lit

df_checked = (
    df_flagged
    .withColumn(
            "ErrorReason",
            when(
                col("CustomerId").isNull(),
                lit("CustomerId is null")
               )
            .when(
                col("IsDuplicate") == True,
                lit("Duplicate CustomerId")
                )
            .when(
                col("CustomerName").isNull() |
                (trim(col("CustomerName")) == ""),
                lit("CustomerName is null or empty")
               )
            .when(
                col("Age").isNull() |
                (col("Age") < 0) |
                (col("Age") > 100),
                lit("Age is invalid")
               )
            .otherwise(lit(None))
                )
                )

display(df_checked)
                                                                                                                                    
           
                                                                                                                                                                                
                                                                                                                                                                                            
                                                                                                                                                                                                       
                                                                                                                                                                                                                    
                                                                                                                                                                                                                          
                                                                                                                                                                                                                                
                                                                                                                                                                                                                                



# COMMAND ----------

# MAGIC %md
# MAGIC ### Separate valid and invalid records

# COMMAND ----------

valid_df = df_checked.filter(col("ErrorReason").isNull())

invalid_df = df_checked.filter(col("ErrorReason").isNotNull())

print("Valid records:", valid_df.count())
print("Invalid records:", invalid_df.count())

display(valid_df)
display(invalid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write valid records to Silver

# COMMAND ----------

valid_df_to_save = valid_df.select(
        "CustomerId",
        "CustomerName",
        "City",
        "Age",
        "UpdatedAt"
)

valid_df_to_save.write.mode("append").saveAsTable(
                            "silver.day18_customers"
                            )

print("Valid records saved to Silver")


# COMMAND ----------

display(
        spark.read.table("silver.day18_customers")
        )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Write invalid records to Quarantine

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

invalid_df_to_save = (
    invalid_df
    .withColumn("RejectedAt", current_timestamp())
    .select(
            "CustomerId",
            "CustomerName",
            "City",
            "Age",
            "UpdatedAt",
            "ErrorReason",
            "RejectedAt"
        )
        )

invalid_df_to_save.write.mode("append").saveAsTable(
                                                    "silver.day18_customer_quarantine"
                                                )

print("Invalid records saved to Quarantine")

# COMMAND ----------

display(
        spark.read.table("silver.day18_customer_quarantine")
        )


# COMMAND ----------

print("Silver count:", spark.read.table("silver.day18_customers").count())
print("Quarantine count:", spark.read.table("silver.day18_customer_quarantine").count())

# COMMAND ----------

display(
        spark.read.table("silver.day18_customers")
            .orderBy("CustomerId")
           
)

# COMMAND ----------

display(
        spark.read.table("silver.day18_customer_quarantine")
            .orderBy("CustomerId")
            
)