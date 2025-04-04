# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake Containers For the Project

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # Get Secrets From Key Vault
    client_id = dbutils.secrets.get(scope = 'formula1-scope',key = 'formula1app-client-id')
    tenant_id = dbutils.secrets.get(scope = 'formula1-scope',key = 'formula1-app-tenant-id')
    client_secret = dbutils.secrets.get(scope = 'formula1-scope',key = 'formula1app-client-secret')

# Set Spark Configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    # Unmount the mount Point If already Exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

# Mount The Storage Account Container
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)
    
    
    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Mount Raw Container

# COMMAND ----------

mount_adls('f1datalake44','raw')

# COMMAND ----------

mount_adls('f1datalake44','processed')

# COMMAND ----------

mount_adls('f1datalake44','presentation')

# COMMAND ----------

