# Databricks-PySpark
End to end project

### Project Overview:


### Requirements
- Create Resource Group and following resources:
    - Databricks Workspace (Premium Tier)
    - Data Lake Gen2 Stoarge Account
    - Key Vault
    - Create Policy, Pool and Cluster for Computation Solution
#### Access Control
- Service Principal (AAD) - Method 1
- SAS Token - Method 2
- Sceret Scope in Databricks
- Key Vault o keep all secrets
- Role Based Access Control (Storage and Key Vault)
#### Mount Storage
- Activate Databrciks File System
- Mount three containers via defined function including AAD
- Check Mounted Storage on top of DBFS
  
![image](https://github.com/user-attachments/assets/81ab7a9e-e203-4386-b5ee-8b5dc92d1e0f)
