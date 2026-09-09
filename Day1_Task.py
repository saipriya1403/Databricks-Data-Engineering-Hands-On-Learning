# Databricks notebook source
print("Welcome to RetailMart Data Engineering")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT current_timestamp();

# COMMAND ----------

customers = [
        (1, "Meera Pandey", "Chennai", 29),
            (2, "Leela kumar", "Delhi", 34),
                (3, "Priya Jerson", "Bangalore", 41),
                    (4, "Arjun Varma", "Coimbatore", 25)
                    ]

columns = ["CustomerId", "Name", "City", "Age"]

df = spark.createDataFrame(customers, columns)

display(df)


# COMMAND ----------

df.show()
df.printSchema()
df.describe().show()
df.count()

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM customers;