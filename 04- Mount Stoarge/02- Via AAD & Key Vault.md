

````python
def mount_adls(Storage, Container):
# Apply Key Vault for secrets
    Client_secret = dbutils.secrets.get(
        scope="Customers-scope", 
        key="KV-ServicePricipal-ClientSecret"
    )
    Client_ID = dbutils.secrets.get(
        scope="Customers-scope", 
        key="KV-ServicePricipal-ClientID"
    )
    Tenant_ID = dbutils.secrets.get(
        scope="Customers-scope", 
        key="KV-ServicePricipal-TenantID"
    )
    
# Set Configurations for mounting    
    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": Client_ID,
        "fs.azure.account.oauth2.client.secret": Client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{Tenant_ID}/oauth2/token"
    }
#Check Mount Available
    if any(mount.mountPoint == f"/mnt/{Storage}/{Container}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{Storage}/{Container}")

# Mount the storage
    dbutils.fs.mount(
        source=f"abfss://{Container}@{Storage}.dfs.core.windows.net/",
        mount_point=f"/mnt/{Storage}/{Container}",
        extra_configs=configs
    )
    
# Display number of mounts
    display(dbutils.fs.mounts())
````

![image](https://github.com/user-attachments/assets/6120f5d5-4590-4287-9ec9-6c6caf5d8b7b)

#Mount Containers with Function
mount_adls("strgdatabricks1", "bronze")
mount_adls("strgdatabricks1", "silver")
mount_adls("strgdatabricks1", "gold")
