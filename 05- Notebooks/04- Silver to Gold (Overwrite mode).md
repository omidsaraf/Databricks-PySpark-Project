````python

%run "./01- Utilities"

df_S=spark.read.format('delta').load('/mnt/strgdatabricks1/silver/Healthcare')
df_S.display()

# Data Frame1
Feeling_Count_Day=df_S.groupBy('FEELING_TODAY','DATE').agg(count('PATIENT_ID').alias('COUNT_Total_PATIENTS')).orderBy('DATE')
Feeling_Count_Day.display()
````
![image](https://github.com/user-attachments/assets/79a5d348-25fe-42ff-b21c-e386163774eb)

````python
# Data Frame2
Symptoms_Count_Day=df_S.groupBy('GENERAL_SYMPTOMS','DATE').agg(count('PATIENT_ID').alias('COUNT_Total_PATIENTS')).orderBy('DATE')
Symptoms_Count_Day.display()
`````
![image](https://github.com/user-attachments/assets/b2adc572-36b1-4871-8305-43570f173ee9)
`````python
# Data Frame3
Healthcare_visit_day=df_S.groupBy('HEALTHCARE_VISIT','DATE').agg(count('PATIENT_ID').alias('COUNT_Total_PATIENTS')).orderBy('DATE')
`````
![image](https://github.com/user-attachments/assets/202e9ab1-c222-4d92-8ad2-84c5de38c1de)
`````python
# Create Gold delta tables

Feeling_Count_Day.write.format('delta').mode('overwrite').option(
    'path', '/mnt/strgdatabricks1/gold/Feeling_Count_Day
).saveAsTable('Healthcare_Gold.Feeling_Count_Day,overwrite=True)

Symptoms_Count_Day.write.format('delta').mode('overwrite').option(
    'path', '/mnt/strgdatabricks1/gold/Symptoms_Count_Day'
).saveAsTable('Healthcare_Gold.Symptoms_Count_Day',overwrite=True)

Healthcare_visit_day.write.format('delta').mode('overwrite').option(
    'path', '/mnt/strgdatabricks1/gold/Healthcare_visit_day'
).saveAsTable('Healthcare_Gold.Healthcare_visit_day',overwrite=True)

dbutils.notebook.exit("Gold Processing Complete")
