# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake Using Service Principal
# MAGIC #### Steps to Follow
# MAGIC 1. Get Client ID,Tenant ID and Client Secret From Key Vault
# MAGIC 2. Set Spark Config with API/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 3. Call File System Utilitymount to mount the storage
# MAGIC 4. Explore other file system utilities related to mount(list all mounts, unmount)

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope',key = 'formula1app-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope',key = 'formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope',key = 'formula1app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@f1datalake44.dfs.core.windows.net/",
  mount_point = "/mnt/f1datalake44/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/f1datalake44/demo"))

# COMMAND ----------

display(spark.read.csv("dbfs:/mnt/f1datalake44/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/f1datalake44/demo')

# COMMAND ----------

