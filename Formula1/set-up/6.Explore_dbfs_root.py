# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore dbfs root
# MAGIC 1. list all the folders in dbfs root
# MAGIC 2. Interact with dbfs file browser
# MAGIC 3. Upload file to dbfs root
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/circuits.csv'))

# COMMAND ----------

display(spark.read.csv('dbfs:/FileStore/circuits.csv'))

# COMMAND ----------

