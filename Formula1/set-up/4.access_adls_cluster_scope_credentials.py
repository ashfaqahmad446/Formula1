# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using clustered scope credentials
# MAGIC 1. set spark config fs.azure.account.key in the cluster
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuts.csv file

# COMMAND ----------

spark.conf.set(
"fs.azure.account.key.formula1adlspractice.dfs.core.windows.net",
"Fgh+oL4GJp4Y2Sq/OSER59V+AX7qgR4W10aOVsEJYh7sjleZaC+97UMMwLbpPQkweINXWOabFrB6+AStgzLf2g=="
)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1adlspractice.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1adlspractice.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("abfss://demo@formula1adlspractice.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1adlspractice.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

