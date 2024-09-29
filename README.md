
Here's a refined and professionally organized version of your project documentation, incorporating additional details from the provided GitHub link:

---

# Databricks-PySpark Health Project

### Project Overview

This project involves managing and processing patient information from hospital visits. The data source is updated daily at 5 PM, retaining the same title and format but containing fresh data each time. Note that there is usually some overlap with the data between days.

### Phase 1: Setup and Configuration

1. **Create Azure Resources:**
   - **Resource Group**
   - **Databricks Workspace (Premium Tier)**
   - **Data Lake Gen2 Storage Account**
   - **Key Vault**

2. **Security Measures:**
   - **Service Principal (AAD) - Method 1**
   - **SAS Token - Method 2**
   - **Secret Scope in Databricks**
   - **Key Vault to store all secrets**
   - **Role-Based Access Control (Storage and Key Vault)**

3. **Databricks Configuration:**
   - **Create Policy, Pool, and Cluster for Computation Solution**

### Phase 2: Data Management Instructions

1. **Mount Storage:**
   - Activate Databricks File System (DBFS)
   - Mount three containers via a defined function including AAD
   - Verify Mounted Storage on DBFS

2. **Data Processing:**
   - **Mount the 'health-updates' container to your DBFS**
   - **Process the 'health_status_updates.csv' file from the bronze folder into the silver folder and call it 'health_data' with an additional column called 'updated_timestamp' consisting of the current timestamp at which the data is inserted into the silver folder**
   - **Store 'health_data' in external Delta Lake format with the underlying data in the silver folder and the table itself as part of a new 'healthcare' database**
   - **Ensure appropriate data types are assigned**

3. **Create Gold Layer Tables:**
   - **Task**: Create tables in the gold layer as external Delta Lake format
   - **Database**: `healthcare`
   - **Data Location**: Gold folder

### Phase 3: Workflow Creation

1. **Upsert Workflow:**
   - **Task**: Upsert (via merge) data from the `health_status_updates.csv` file in the bronze folder into the silver `health_data` table
   - **Matching Criteria**: Match records based on the `status_update_id` column of both tables
   - **Task**: Ensure newly updated or inserted records have an updated timestamp value

### Phase 4: Daily Health Updates - ETL Pipeline

**Steps:**

1. **Extract:**
   - Fetch the new health updates file from the source
   - Ensure the file format and title are consistent with the previous day's file

2. **Transform:**
   - Process the new data to handle any overlaps with the previous day's data
   - Clean and format the data as required

3. **Load:**
   - Replace the old file in the bronze folder with the new file
   - Ensure the new file is correctly loaded and accessible for further processing

**Schedule:**
- The pipeline is scheduled to run daily at 5 PM

**Notes:**
- Overlapping data between days is expected and handled during the transformation step


# Databricks-PySpark
Health project

### Project Overview:
This project is related to the information of patients who have visited the hospital. The dataset includes three folders with CSV files in the same format. Some of the dates in each file overlap with each other.

### Phase 1: Data Management Instructions

1. **Mount the 'health-updates' container to your DBFS.**
2. **Process the 'health_status_updates.csv' file from the bronze folder into the silver folder and call it 'health_data' with an additional column called 'updated_timestamp' consisting of the current_timestamp at which the data is inserted into the silver folder.**
3. **'health_data' should be in external delta lake format with the underlying data in the silver folder and the table itself as part of a new 'healthcare' database.**
4. **Ensure the data types are appropriately assigned.**


### Phase 2: Create Workflow

1. **Create a Workflow for Upserts**
   - **Task**: Upsert (via merge) data from the `health_status_updates.csv` file in the bronze folder into the silver `health_data` table.
   - **Matching Criteria**: Match records based on the `status_update_id` column of both tables.

2. **Timestamp Updates**
   - **Task**: Ensure newly updated or inserted records have an updated timestamp value.

3. **Create Gold Layer Tables**
   - **Task**: Create tables in the gold layer as external Delta Lake format.
   - **Database**: `healthcare`
   - **Data Location**: Gold folder

### Phase 3: Daily Health Updates ETL Pipeline

**Description:**
This ETL pipeline is designed to run daily at 5 PM. It replaces the existing health updates file in the bronze folder with new data for the next day. The file retains the same title and format but contains fresh data each time. Note that there is usually some overlap with the data between days.

**Steps:**
1. **Extract:**
   - Fetch the new health updates file from the source.
   - Ensure the file format and title are consistent with the previous day's file.

2. **Transform:**
   - Process the new data to handle any overlaps with the previous day's data.
   - Clean and format the data as required.

3. **Load:**
   - Replace the old file in the bronze folder with the new file.
   - Ensure the new file is correctly loaded and accessible for further processing.

**Schedule:**
- The pipeline is scheduled to run daily at 5 PM.

**Notes:**
- Overlapping data between days is expected and handled during the transformation step.




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
