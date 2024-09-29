````python
%run "./01- Utilities"

#Read new file
path_B = 'dbfs:/mnt/strgdatabricks1/health/bronze/*.csv'
#df = spark.read.csv(path1, header=True, inferSchema=True)
#display(df)

#define schema
schema = StructType([
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
df=spark.read.csv(path_B, header=True, schema=schema)
df=df.withColumn('Date', to_date('DATE_PROVIDED', 'MM/dd/yyyy')).drop('DATE_PROVIDED')\
    .withColumn('Updated_timestamp', current_timestamp())
#df.display()
اr
#Merge Operation - Spark method
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp
##define deltaTable As Target Table
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

dbutils.notebook.exit("Silver Processing Complete")
