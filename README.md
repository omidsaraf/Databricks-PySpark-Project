
# Databricks-PySpark 
*Health Project*

### Project Overview


This project involves managing and processing patient information from hospital visits. The data source is updated daily at 5 PM, retaining the same title and format but containing fresh data each time. Note that there is usually some overlap with the data between days.

### Daily Health Updates - ETL Pipeline

1. **Extract:**
   - Fetch the new health updates file from the source.
   - Ensure the file format and title are consistent with the previous day's file.

2. **Transform:**
   - Process the new data to handle any overlaps with the previous day's data.
   - Clean and format the data as required.

3. **Load:**
   - Replace the old file in the bronze folder with the new file.
   - Ensure the new file is correctly loaded and accessible for further processing.

### Phase 1: Setup and Configuration

 **Create Azure Resources:**
   - **Resource Group**
   - **Databricks Workspace (Premium Tier)**
   - **Data Lake Gen2 Storage Account**
   - **Key Vault**

 **Security Measures:**
   - **Service Principal (AAD) - Method 1**
   - **SAS Token - Method 2**
   - **Secret Scope in Databricks**
   - **Key Vault to store all secrets**
   - **Role-Based Access Control (Storage and Key Vault)**

 **Databricks Configuration:**
   - **Create Policy, Pool, and Cluster for Computation Solution**

### Phase 2: Data Management Instructions

 **Mount Storage:**
   - Activate Databricks File System (DBFS)
   - Mount three containers via a defined function including AAD
   - Verify Mounted Storage on DBFS

 **Create Databases:**
   - Create a new database named `Healthcare_Silver` & `Healthcare_Gold` for organizing tables.
   - Refer to the [Create Databases Notebook](https://github.com/omidsaraf/Databricks-PySpark/blob/main/05-%20Notebooks/02-%20Create%20Databases.md) for detailed steps.

 **Create Notebooks:**
   - Utility Notebook
     - Refer to the [Utility Notebook](https://github.com/omidsaraf/Databricks-PySpark/blob/main/05-%20Notebooks/01-%20Utility%20Notebook) for reusable functions and configurations used across different notebooks.

   - Initial Data Load (Bronze to Silver):
     - Create a Dataframe to read CSV File
     - Create Schema
     - Process, Transform and add `updated_timestamp` into the dataframe
     - Write Dataframe in external Delta Lake format with the underlying data in the silver folder and the table itself
     - Note: do not use overwrite mode
     - Refer to the [Bronze to Silver (Initial Load) Notebook](https://github.com/omidsaraf/Databricks-PySpark/blob/main/05-%20Notebooks/03-%20Bronze%20to%20Silver%20(initial%20load).md)
   - Silver to Gold:
     - Create three Dataframes from file sitted in Silver Layer
     - Create Schemas for them
     - Process, Transform and add `updated_timestamp` into the dataframes.
     - Write Dataframes in external Delta Lake format with the underlying data in the Gold folder and the tables
     - Note: use overwrite mode
     - Refer to the [Silver to Gold (Overwrite Mode) Notebook](https://github.com/omidsaraf/Databricks-PySpark/blob/main/05-%20Notebooks/04-%20Silver%20to%20Gold%20(Overwrite%20mode).md)    - Incremental Data Load (Bronze to Silver):
     - Incrementally load new data from the bronze folder into the silver `health_data` table.
     - Upsert (via merge) data from the `health_status_updates.csv` file in the bronze folder into the silver `health_data` table.
     - Match records based on the `status_update_id` column.
     - Ensure newly updated or inserted records have an updated timestamp value.
     - Refer to the [Bronze to Silver (Incremental Load) Notebook](https://github.com/omidsaraf/Databricks-PySpark/blob/main/05-%20Notebooks/06-%20Bronze%20to%20Silver%20Notebook%20(incremental%20load).md)
    - Master Notebook:
     - this notebook will be run daily with trigger and rund Incremental load notebooks.

### Phase 3: Detailed Workflow Execution

1. **Master Notebook Execution:**
   - The Master Notebook orchestrates the entire ETL pipeline.
   - It sequentially runs the dependent notebooks for each phase of the ETL process.
   - During each run, it ensures the following:
     - **Bronze to Silver (Incremental Load)**: Merges new data into the silver `health_data` table.
     - **Silver to Gold (Overwrite Mode)**: Overwrites the gold layer tables with the latest aggregated and business-ready data.

2. **Bronze to Silver (Incremental Load):**
   - **Task**: Incrementally load new data from the bronze folder into the silver `health_data` table.
   - **Matching Criteria**: Match records based on the `status_update_id` column.
   - **Transformation**: Add an `updated_timestamp` column with the current timestamp.
   - **Mode**: Merge

3. **Silver to Gold (Overwrite Mode):**
   - **Task**: Overwrite the gold layer tables with the latest data from the silver layer.
   - **Transformation**: Perform necessary aggregations and calculations.
   - **Mode**: Overwrite


**Schedule:**
- The pipeline is scheduled to run daily at 5 PM.

**Notes:**
- Overlapping data between days is expected and handled during the transformation step.


![image](https://github.com/user-attachments/assets/81ab7a9e-e203-4386-b5ee-8b5dc92d1e0f)
