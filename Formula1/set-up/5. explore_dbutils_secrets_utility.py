# Databricks notebook source
# MAGIC %md
# MAGIC ##Explore the capabilities of the dbutils.secrets utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('Formula1-seceretScope')

# COMMAND ----------

dbutils.secrets.get('Formula1-seceretScope','Formula1-dl-account-key')

# COMMAND ----------

