
````python
#run Bronze to Silver notebook - Pass Parameter
dbutils.notebook.run("/Workspace/Users/niloofar.kandi@nexondemo.nexon.com.au/Exercise1-Malvik/Health-project/ETL/05- Bronze to Silver (Incremental)", 60)

#run Silver to Gold notebook - Pass Parameter
dbutils.notebook.run("/Workspace/Users/niloofar.kandi@nexondemo.nexon.com.au/Exercise1-Malvik/Health-project/ETL/06- Silver to Gold", 60)
