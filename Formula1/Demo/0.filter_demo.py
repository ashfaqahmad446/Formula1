# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
                        .withColumnRenamed("name", "circuits_name")  

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019") \
                            .filter("circuit_Id > 70")
                            .withColumnRenamed("name", "races_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

