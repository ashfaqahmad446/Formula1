# Databricks notebook source
# MAGIC %md
# MAGIC ##### Access dataframe using SQL
# MAGIC ###### 1. Create Global Temp view from SQL Cell
# MAGIC ###### 2. Access the view from SQL Cell
# MAGIC ###### 3. Access the view from Python Cell
# MAGIC ###### 4. Access the view from another notebook

# COMMAND ----------

# MAGIC %run "..//includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql 
# MAGIC show tables in global_temp;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from global_temp.gv_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1)
# MAGIC from global_temp.gv_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

# MAGIC %md
# MAGIC ##### To execute SQL from python use spark.sql....
# MAGIC ###### this give you the ability to put the result of the view in dataFrame and then use the dataframe API to use it. secondly you can you variables inside the code

# COMMAND ----------

p_race_year = 2019

# COMMAND ----------

race_results_2019_df = spark.sql(f"select * from global_temp.gv_race_results where race_year = {p_race_year}")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

