
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
   - Refer to the [Utility Notebook](https://github.com/omidsaraf/Databricks-PySpark/blob/main/05-%20Notebooks/01-%20Utility%20Notebook) for reusable functions and configurations.

2. **Create Databases:**
   - Create a new database named `healthcare` for organizing tables.
   - Refer to the [Create Databases Notebook](https://github.com/omidsaraf/Databricks-PySpark/blob/main/05-%20Notebooks/02-%20Create%20Databases.md) for detailed steps.

3. **Initial Data Load (Bronze to Silver):**
   - **Mount the 'health-updates' container to your DBFS.**
   - **Process the 'health_status_updates.csv' file from the bronze folder into the silver folder and call it 'health_data' with an additional column called 'updated_timestamp' consisting of the current timestamp at which the data is inserted into the silver folder.**
   - **Store 'health_data' in external Delta Lake format with the underlying data in the silver folder and the table itself as part of the 'healthcare' database.**
   - **Ensure appropriate data types are assigned.**
   - Refer to the [Bronze to Silver (Initial Load) Notebook](https://github.com/omidsaraf/Databricks-PySpark/blob/main/05-%20Notebooks/03-%20Bronze%20to%20Silver%20(initial%20load).md) for detailed steps.

4. **Create Gold Layer Tables:**
   - **Task**: Create tables in the gold layer as external Delta Lake format.
   - **Database**: `healthcare`
   - **Data Location**: Gold folder
   - **Mode**: Overwrite
   - Refer to the [Silver to Gold (Overwrite Mode) Notebook](https://github.com/omidsaraf/Databricks-PySpark/blob/main/05-%20Notebooks/04-%20Silver%20to%20Gold%20(Overwrite%20mode).md) for detailed steps.

### Phase 3: Workflow Creation

1. **Upsert Workflow:**
   - **Task**: Upsert (via merge) data from the `health_status_updates.csv` file in the bronze folder into the silver `health_data` table.
   - **Matching Criteria**: Match records based on the `status_update_id` column of both tables.
   - **Task**: Ensure newly updated or inserted records have an updated timestamp value.
   - Refer to the [Bronze to Silver (Incremental Load) Notebook](https://github.com/omidsaraf/Databricks-PySpark/blob/main/05-%20Notebooks/06-%20Bronze%20to%20Silver%20Notebook%20(incremental%20load).md) for detailed steps.

### Phase 4: Daily Health Updates - ETL Pipeline

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
- Refer to the [Master Notebook](https://github.com/omidsaraf/Databricks-PySpark/blob/main/05-%20Notebooks/07-%20Master%20Notebook.md) for orchestrating the entire ETL pipeline.

### Detailed Workflow Execution

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
   - Refer to the [Bronze to Silver (Incremental Load) Notebook](https://github.com/omidsaraf/Databricks-PySpark/blob/main/05-%20Notebooks/06-%20Bronze%20to%20Silver%20Notebook%20(incremental%20load).md) for detailed steps.

3. **Silver to Gold (Overwrite Mode):**
   - **Task**: Overwrite the gold layer tables with the latest data from the silver layer.
   - **Transformation**: Perform necessary aggregations and calculations.
   - **Mode**: Overwrite
   - Refer to the [Silver to Gold (Overwrite Mode) Notebook](https://github.com/omidsaraf/Databricks-PySpark/blob/main/05-%20Notebooks/04-%20Silver%20to%20Gold%20(Overwrite%20mode).md) for detailed steps.

### Utility Functions

- Refer to the [Utility Notebook](https://github.com/omidsaraf/Databricks-PySpark/blob/main/05-%20Notebooks/01-%20Utility%20Notebook) for reusable functions and configurations used across different notebooks.

### Additional Information

For more details, refer to the [project repository](https://github.com/omidsaraf/Databricks-PySpark).

---

Feel free to copy this into your GitHub repository. Let me know if there's anything else you need!

Source: Conversation with Copilot, 29/09/2024
(1) Best practices around bronze/silver/gold (medallio ... - Databricks. https://community.databricks.com/t5/data-engineering/best-practices-around-bronze-silver-gold-medallion-model-data/td-p/26044.
(2) What is a Medallion Architecture? - Databricks. https://www.databricks.com/glossary/medallion-architecture.
(3) Real-Time EDW Modeling with Databricks | Databricks Blog. https://www.databricks.com/blog/2022/11/07/load-edw-dimensional-model-real-time-databricks-lakehouse.html.
(4) How to implement Slowly Changing Dimensions when y ... - Databricks. https://community.databricks.com/t5/technical-blog/how-to-implement-slowly-changing-dimensions-when-you-have/ba-p/43937.
(5) Solved: Trigger one workflow after completion of another w ... - Databricks. https://community.databricks.com/t5/data-engineering/trigger-one-workflow-after-completion-of-another-workflow/td-p/65864.
(6) Schedule and orchestrate Workflows | Databricks on AWS. https://docs.databricks.com/en/jobs/index.html.
(7) Run jobs on a schedule or continuously | Databricks on AWS. https://docs.databricks.com/en/jobs/schedule-jobs.html.
(8) Implement Spark Pipelines with Notebooks | Databricks Blog. https://www.databricks.com/blog/2016/08/30/notebook-workflows-the-easiest-way-to-implement-apache-spark-pipelines.html.
(9) Gracefully stop a job based on condition - Databricks. https://community.databricks.com/t5/data-engineering/gracefully-stop-a-job-based-on-condition/td-p/37610.

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
