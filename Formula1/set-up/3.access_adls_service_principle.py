# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using service principle
# MAGIC 1. Register Azure AD/Service principle
# MAGIC 2. Generate a seceret/password for application
# MAGIC 3. Set spark config with app/client id, Directory/Tenant ID and seceret
# MAGIC 4. Assign role "Storage data contributer" to the storage account

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

client_id = dbutils.secrets.get('Formula1-seceretScope','formula1-app-client-id')
tenant_id = dbutils.secrets.get('Formula1-seceretScope','formula1-app-tenant-id')
client_secret = dbutils.secrets.get('Formula1-seceretScope','formula1-app-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1adlspractice.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1adlspractice.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1adlspractice.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1adlspractice.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1adlspractice.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1adlspractice.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1adlspractice.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("abfss://demo@formula1adlspractice.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1adlspractice.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

