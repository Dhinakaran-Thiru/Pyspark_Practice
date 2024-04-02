from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()


data = [("dhina",34), ("deepika",56),
        ("naveen",30), ("praveen",24) ]

df1 = spark.createDataFrame(data = data, schema=["name","id"])
df1.printSchema()


data2=[(34,"James"),(45,"Maria"),
       (45,"Jen"),(34,"Jeff")]

df2 = spark.createDataFrame(data = data2, schema = ["id","name"])
df2.printSchema()

df1.unionByName(df2).show()

df3=[("dhina",21,"dataengineer"),
     ("deepak",22,"softwareengineer")]
schema10=("name","age","designation")
df4=[("deepa",45,"sister"),
     ("deepika",56,"sister")]
schema11=("name","age","relation")
df5=spark.createDataFrame(data=df3,schema=schema10)
df5.show()
df6=spark.createDataFrame(data=df4,schema=schema11)
df6.show()
df5.unionByName(df6,allowMissingColumns=True).show() #here allowmissingcolumn we need to put null values over there
