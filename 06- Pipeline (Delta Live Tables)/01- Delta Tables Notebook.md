```python

# Import necessary libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt
from delta.tables import DeltaTable

# Data Discovery
path_B = 'dbfs:/mnt/strgdatabricks1/bronze/*.csv'

# Define schema
schema_health = StructType([
    StructField("STATUS_UPDATE_ID", IntegerType(), False),
    StructField("PATIENT_ID", IntegerType(), False),
    StructField("DATE_PROVIDED", StringType(), False),
    StructField("FEELING_TODAY", StringType(), True),
    StructField("IMPACT", StringType(), True),
    StructField("INJECTION_SITE_SYMPTOMS", StringType(), True),
    StructField("HIGHEST_TEMP", DoubleType(), True),
    StructField("FEVERISH_TODAY", StringType(), True),
    StructField("GENERAL_SYMPTOMS", StringType(), True),
    StructField("HEALTHCARE_VISIT", StringType(), True)
])

df = spark.read.csv(path_B, header=True, schema=schema_health)
df = df.withColumn('Date', to_date('DATE_PROVIDED', 'MM/dd/yyyy')).drop('DATE_PROVIDED')\
    .withColumn('Updated_timestamp', current_timestamp())
df.display()

# Bronze Table
@dlt.table
def health_bronze():
    return spark.read.format("csv").option("header", "true").schema(schema_health).load(path_B)

# Silver Table
@dlt.table
def health_silver():
    return (
        dlt.read('health_bronze')
        .withColumn('Date', to_date('DATE_PROVIDED', 'MM/dd/yyyy')).drop('DATE_PROVIDED')
        .withColumn('Updated_timestamp', current_timestamp())
    )

# Assuming df is the DataFrame you want to merge with the Delta table
df = health_silver()

deltaTable = DeltaTable.forPath(spark, '/mnt/strgdatabricks1/health/silver/Healthcare')

deltaTable.alias('T')\
    .merge(
        df.alias('S'), 
        'T.STATUS_UPDATE_ID = S.STATUS_UPDATE_ID'
    )\
    .whenMatchedUpdate(
        set={
            'STATUS_UPDATE_ID': 'S.STATUS_UPDATE_ID',
            'PATIENT_ID': 'S.PATIENT_ID',
            'Date': 'S.Date',
            'FEELING_TODAY': 'S.FEELING_TODAY',
            'IMPACT': 'S.IMPACT',
            'INJECTION_SITE_SYMPTOMS': 'S.INJECTION_SITE_SYMPTOMS',
            'HIGHEST_TEMP': 'S.HIGHEST_TEMP',
            'FEVERISH_TODAY': 'S.FEVERISH_TODAY',
            'GENERAL_SYMPTOMS': 'S.GENERAL_SYMPTOMS',
            'HEALTHCARE_VISIT': 'S.HEALTHCARE_VISIT',
            'Updated_timestamp': current_timestamp()
        }
    )\
    .whenNotMatchedInsert(
        values={
            'STATUS_UPDATE_ID': 'S.STATUS_UPDATE_ID',
            'PATIENT_ID': 'S.PATIENT_ID',
            'Date': 'S.Date',
            'FEELING_TODAY': 'S.FEELING_TODAY',
            'IMPACT': 'S.IMPACT',
            'INJECTION_SITE_SYMPTOMS': 'S.INJECTION_SITE_SYMPTOMS',
            'HIGHEST_TEMP': 'S.HIGHEST_TEMP',
            'FEVERISH_TODAY': 'S.FEVERISH_TODAY',
            'GENERAL_SYMPTOMS': 'S.GENERAL_SYMPTOMS',
            'HEALTHCARE_VISIT': 'S.HEALTHCARE_VISIT',
            'Updated_timestamp': current_timestamp()
        }
    )\
    .execute()

# Gold Tables
@dlt.table
def health_gold_Feeling_Count_Day():
    return (
        dlt.read('health_silver')
        .groupBy('FEELING_TODAY', 'Date')
        .agg(count('PATIENT_ID').alias('COUNT_Total_PATIENTS'))
        .orderBy('Date')
    )

@dlt.table
def health_gold_Symptoms_Count_Day():
    return (
        dlt.read('health_silver')
        .groupBy('GENERAL_SYMPTOMS', 'Date')
        .agg(count('PATIENT_ID').alias('COUNT_Total_PATIENTS'))
        .orderBy('Date')
    )

@dlt.table
def health_gold_Healthcare_visit_day():
    return (
        dlt.read('health_silver')
        .groupBy('HEALTHCARE_VISIT', 'Date')
        .agg(count('PATIENT_ID').alias('COUNT_Total_PATIENTS'))
        .orderBy('Date')
    )
