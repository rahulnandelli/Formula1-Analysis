# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake Using Service Principal
# MAGIC #### Steps to Follow
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/ password for the Application
# MAGIC 3. Set Spark Config with API/ Client Id, Diretoctory/ Tenant Id & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake.

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope',key = 'formula1app-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope',key = 'formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope',key = 'formula1app-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1datalake44.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.f1datalake44.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.f1datalake44.dfs.core.windows.net",client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.f1datalake44.dfs.core.windows.net",client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.f1datalake44.dfs.core.windows.net",f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1datalake44.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1datalake44.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

