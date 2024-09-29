````python

#Data Discovery

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
df.display()
