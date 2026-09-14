# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day5_customers
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Incoming Customer Data

# COMMAND ----------

incoming_customers = [
        (1, "Arjun", "Coimbatore", 30),
        (3, "Michael", "Chennai", 35),
        (9, "Divya", "Madurai", 28)
]

columns = [
          "CustomerId",
          "CustomerName",
          "City",
           "Age"
]

incoming_df = spark.createDataFrame(incoming_customers, columns)
display(incoming_df)                                
                                        





# COMMAND ----------

# MAGIC %md
# MAGIC ### Store Incoming Data in Bronze

# COMMAND ----------

incoming_df.write \
        .format("delta") \
         .mode("overwrite") \
         .saveAsTable("bronze.day6_customer_updates")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze.day6_customer_updates
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform the MERGE

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.day5_customers AS target
# MAGIC USING bronze.day6_customer_updates AS source
# MAGIC ON target.CustomerId = source.CustomerId
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.CustomerName = source.CustomerName,
# MAGIC     target.City = source.City,
# MAGIC     target.Age = source.Age
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC             CustomerId,CustomerName,City,Age)
# MAGIC             VALUES (
# MAGIC                 source.CustomerId,
# MAGIC                 source.CustomerName,
# MAGIC                 source.City,
# MAGIC                 source.Age);
# MAGIC                                              
# MAGIC                                                                                           
# MAGIC                                                                                                                                            
# MAGIC                                                                                                                                                                                              
# MAGIC                                                                                                                                                                                                                                                       
# MAGIC
# MAGIC    
# MAGIC                                       
# MAGIC                                                                                
# MAGIC                                                                                                                               
# MAGIC                                                                                                                                                                                      
# MAGIC                                                                                                                                                                                                                                                  
# MAGIC                                                                                                                                                                                                                                                              
# MAGIC                                                                                                                                                                                                                                                                                
# MAGIC                                                                                                                                                                                                                                                                                                              
# MAGIC                                                                                                                                                                                                                                                                                                                                              
# MAGIC                                                                                                                                                                                                                                                                                                                                                                       
# MAGIC                                                                                                                                                                                                                                                                                                                                                                                                                       
# MAGIC                                                                                                                                                                                                                                                                                                                                                                                                                                                            
# MAGIC
# MAGIC                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
# MAGIC                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
# MAGIC                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
# MAGIC                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
# MAGIC                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
# MAGIC          
# MAGIC                            
# MAGIC                               
# MAGIC                                 
# MAGIC                          
# MAGIC                                
# MAGIC                                      
# MAGIC                                           
# MAGIC                                              
# MAGIC                                                       

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the MERGE Result

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day5_customers
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Delta Transaction History

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver.day5_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understand WHEN MATCHED

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day5_customers
# MAGIC WHERE CustomerId IN (1, 3)
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understand WHEN NOT MATCHED

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day5_customers
# MAGIC WHERE CustomerId = 9;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Batch Date

# COMMAND ----------

incoming_customers_2 = [
        (2, "Priya", "Bangalore", 27, "2026-09-14"),
            (9, "Divya", "Chennai", 29, "2026-09-14")
            ]

columns_2 = [
            "CustomerId",
            "CustomerName",
            "City",
            "Age",
            "BatchDate"]


incoming_df_2 = spark.createDataFrame(
                                    incoming_customers_2,
                                        columns_2
                                        )

display(incoming_df_2)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Final Verification

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day5_customers
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver.day5_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC