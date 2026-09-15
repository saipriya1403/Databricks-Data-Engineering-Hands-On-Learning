# Databricks notebook source
# MAGIC %md
# MAGIC ### Create Day 7 test data

# COMMAND ----------

customers_day7 = [
        (1, "Arjun", "arjun@gmail.com", "Chennai", 29),
        (2, "Priya", "priya@gmail.com", "Bangalore", 26),
        (3, "Michael", "michael@gmail.com", "Hyderabad", 34),
        (4, "Neha", "neha@gmail.com", "Coimbatore", 31),
        (5, "Vikram", "vikram@gmail.com", "Madurai", 38),
        (6, "Ananya", "ananya@gmail.com", "Salem", 24),
        (7, "Rahul", "rahul@gmail.com", "Trichy", 32),
        (7, "Rahul", "rahul@gmail.com", "Trichy", 32),
        (8, None, "sneha@gmail.com", "Erode", 27),
        (9, "Karthik", None, "Tirunelveli", 36),
        (10, "Divya", "divya@gmail.com", None, 28),
        (11, "Nithya", "nithya@gmail.com", "Chennai", -5),
        (12, " ", "blank@gmail.com", "Chennai", 30),
        (13, "Surya", "surya@gmail.com", "Chennai", 150),
        (None, "Meena", "meena@gmail.com", "Chennai", 29)
]

columns = [
          "CustomerId","CustomerName","Email","City","Age"]
df =spark.createDataFrame(customers_day7,columns)
display(df)                                                                     
                                                                           
                                                                                
                                                                                    
                                                                                    

                                                                          

                                                                            


# COMMAND ----------

# MAGIC %md
# MAGIC ### Store data in Bronze

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable(
        "bronze.day7_customer_quality"
        )


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.day7_customer_quality
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check NULL values

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS TotalRecords,
# MAGIC     SUM(CASE WHEN CustomerId IS NULL THEN 1 ELSE 0 END) AS NullCustomerId,
# MAGIC     SUM(CASE WHEN CustomerName IS NULL THEN 1 ELSE 0 END) AS NullCustomerName,
# MAGIC     SUM(CASE WHEN Email IS NULL THEN 1 ELSE 0 END) AS NullEmail,
# MAGIC     SUM(CASE WHEN City IS NULL THEN 1 ELSE 0 END) AS NullCity,
# MAGIC     SUM(CASE WHEN Age IS NULL THEN 1 ELSE 0 END) AS NullAge
# MAGIC FROM bronze.day7_customer_quality;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detect duplicate CustomerIds

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     CustomerId,
# MAGIC         COUNT(*) AS RecordCount
# MAGIC         FROM bronze.day7_customer_quality
# MAGIC         WHERE CustomerId IS NOT NULL
# MAGIC         GROUP BY CustomerId
# MAGIC         HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detect invalid ages

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM bronze.day7_customer_quality
# MAGIC WHERE Age < 0
# MAGIC    OR Age > 120;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detect empty CustomerName

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM bronze.day7_customer_quality
# MAGIC WHERE CustomerName IS NULL
# MAGIC    OR TRIM(CustomerName) = '';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define our Data Quality Rules
# MAGIC Rule 1: CustomerId cannot be NULL
# MAGIC
# MAGIC Rule 2: CustomerId must be unique
# MAGIC
# MAGIC Rule 3: CustomerName cannot be NULL or empty
# MAGIC
# MAGIC Rule 4: Email cannot be NULL
# MAGIC
# MAGIC Rule 5: City cannot be NULL or empty
# MAGIC
# MAGIC Rule 6: Age must be between 0 and 120

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Clean Silver Data

# COMMAND ----------

from pyspark.sql.functions import col, trim
clean_df = (
        df
        .filter(col("CustomerId").isNotNull())
        .filter(col("CustomerName").isNotNull())
        .filter(trim(col("CustomerName")) != "")
        .filter(col("Email").isNotNull())
        .filter(col("City").isNotNull())
        .filter(trim(col("City")) != "")
        .filter((col("Age") >= 0) & (col("Age") <= 120))
        .dropDuplicates(["CustomerId"])
)

display(clean_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Valid Records to Silver

# COMMAND ----------

clean_df.write.format("delta").mode("overwrite").saveAsTable(
        "silver.day7_customers")


       


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM silver.day7_customers
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Invalid/Quarantine Records

# COMMAND ----------

invalid_df = (
        df
        .filter(
        col("CustomerId").isNull()
        | col("CustomerName").isNull()
        | (trim(col("CustomerName")) == "")
        | col("Email").isNull()
        | col("City").isNull()
        | (trim(col("City")) == "")
        | (col("Age") < 0)
        | (col("Age") > 120)))
display(df)                                                                               
                                                                                




# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Invalid Records

# COMMAND ----------

invalid_df.write.format("delta").mode("overwrite").saveAsTable(
        "silver.day7_customer_quarantine"
        )


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM silver.day7_customer_quarantine;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Capture Duplicate Records

# COMMAND ----------

duplicate_customer_ids = (
        df
        .filter(col("CustomerId").isNotNull())
        .groupBy("CustomerId")
        .count()
        .filter(col("count") > 1)
)

display(duplicate_customer_ids)


# COMMAND ----------

duplicate_df = (
        df
        .join(duplicate_customer_ids, on="CustomerId", how="inner")
        .drop("count")
)

display(duplicate_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Duplicate Records to Quarantine

# COMMAND ----------

duplicate_df.write.format("delta").mode("append").saveAsTable(
        "silver.day7_customer_quarantine"
        )


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM silver.day7_customer_quarantine
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final Silver Verification

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM silver.day7_customers
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) AS ValidRecords
# MAGIC FROM silver.day7_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final Quarantine Verification

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM silver.day7_customer_quarantine
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) AS QuarantinedRecords
# MAGIC FROM silver.day7_customer_quarantine;

# COMMAND ----------

# MAGIC %md
# MAGIC