# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SSAS Token
# MAGIC 1. set spark config for SSAS Token
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuts.csv file

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('Formula1-seceretScope')

# COMMAND ----------

formula1dl_demo_ssas_token = dbutils.secrets.get(scope='Formula1-seceretScope', key='Formula1-Demo-SSAS-Token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1adlspractice.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1adlspractice.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1adlspractice.dfs.core.windows.net", formula1dl_demo_ssas_token)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1adlspractice.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1adlspractice.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("abfss://demo@formula1adlspractice.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1adlspractice.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

