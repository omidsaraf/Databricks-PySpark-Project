````python


spark.conf.set("fs.azure.account.auth.type.strgdatabricks1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.strgdatabricks1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.strgdatabricks1.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-08-24T11:12:08Z&st=2024-08-24T03:12:08Z&spr=https&sig=f5YAXsUyI341zeXg0XEefrb1T7Lw2NcDmQ%2B3sUCUF5Y%3D")


storageAccountName = "charlesdatabricksadlsno"
storageAccountAccessKey = <access-key>
sasToken = <sas-token>
blobContainerName = "aaa"
mountPoint = "/mnt/data/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
      source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
      mount_point = mountPoint,
      #extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
      extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)
