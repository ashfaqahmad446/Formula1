# Databricks notebook source
# MAGIC %fs
# MAGIC ls

# COMMAND ----------

dbutils.fs.ls('dbfs:/databricks-datasets/COVID')

# COMMAND ----------

for files in dbutils.fs.ls('dbfs:/databricks-datasets/'):
    if files.name.endswith('/'):
        print(files.name)

# COMMAND ----------

dbutils.fs.help('ls')

# COMMAND ----------

