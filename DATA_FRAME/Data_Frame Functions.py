from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import count
spark=SparkSession.builder.appName("main Dataframe Functions").getOrCreate()
data=[("simon",45,"singarapettai",20000),
      ("sah jahan",46,"nadupatti",75000),
      ("noor jahan",47,"uthangarai",56000),
      ("venpa",87,"thirupathur",78000)]

custom_Schema=StructType([StructField("Name",StringType(),True),
                          StructField("age",IntegerType(),True),
                          StructField("Place",StringType(),True),
                          StructField("Salary",IntegerType(),True)])
dataframe=spark.createDataFrame(data=data,schema=custom_Schema)
dataframe.show()

#dataframe1=spark.createDataFrame(data=[],schema=StructType([])).show() #Creation of Empty Dataframe

'''print(dataframe.count()) #row Count
print(len(dataframe.columns)) #Column Count
print(dataframe.distinct().count()) #distinct Count()
dataframe.select(count(dataframe.Name)).show() #particular column count #for this this we have to import count functions
dataframe.select(count(dataframe.Name),count(dataframe.Place)).show() #show multiple column count
dataframe.groupBy('age').count().show()  #using groupBy\

#select function
dataframe.select("*").show()
dataframe.select("Name","Place").show()
dataframe.select(dataframe.Name,dataframe.Place).show()
dataframe.select(dataframe['Name'],dataframe['Place']).show()
#dataframe.select("name.*").show() #truncate is used for column character length'''

#filter and Where
dataframe.filter(dataframe.Name =="simon").show()
#dataframe.filter((dataframe.name =="simon") & (dataframe.age ==87)).show()










