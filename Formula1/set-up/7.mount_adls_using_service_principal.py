# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake using Service Principal
# MAGIC ##### Steps to follow
# MAGIC 1. Get client_id, tenant_id and client_secret from key valult
# MAGIC 2. Set Sark config with App/client Id, Directory/Tenant Id &Secret
# MAGIC 3. call file system utility mount to mount the storage
# MAGIC 4. Explore other file system utilities related to mount (list all munts, unmounts)

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

client_id = dbutils.secrets.get('Formula1-seceretScope','formula1-app-client-id')
tenant_id = dbutils.secrets.get('Formula1-seceretScope','formula1-app-tenant-id')
client_secret = dbutils.secrets.get('Formula1-seceretScope','formula1-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1adlspractice.dfs.core.windows.net/",
  mount_point = "/mnt/formula1adlspractice/demo",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1adlspractice/demo")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1adlspractice/demo"))

# COMMAND ----------

spark.read.csv("dbfs:/mnt/formula1adlspractice/demo/circuits.csv")

# COMMAND ----------

display(spark.read.csv("dbfs:/mnt/formula1adlspractice/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/formula1adlspractice/demo')

# COMMAND ----------

