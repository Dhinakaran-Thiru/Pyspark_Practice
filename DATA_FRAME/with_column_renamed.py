from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("with column renamed").getOrCreate()
data=[(1,"dhina",56768),(2,"deepak",566778),(3,"divakar",45666)]
schema=["s.no","name","salary"]
df=spark.createDataFrame(data=data,schema=schema)
df.show()
df1=df.withColumnRenamed('s.no','id_no')
df1.show()