

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




