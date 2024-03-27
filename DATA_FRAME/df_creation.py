from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
spark=SparkSession.builder.appName("df creation").getOrCreate()
data=[('dhina','karan',12,'student'),
      ('sachin','tendulkhar',56,'data engineer'),
      ('deepa','kamal',28,'teacher'),
      ('deepika','',24,'beautician')]

schema=StructType([
    StructField("firstname",StringType(),True),
    StructField("lastname",StringType(),True),
    StructField("age",IntegerType(),True),
    StructField("designation",StringType(),True)
])
df=spark.createDataFrame(data=data,schema=schema)
df.show()

'''from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
spark=SparkSession.builder.appName("creation of another dataframe").getOrCreate()'''
data= [("deepika","female",34,"beautician"),
       ("deepa","female",36,"teacher"),
       ("dhinakaran","male",20,"Data Engineer")]

datum=StructType([StructField("name",StringType(),True),
                  StructField("gender",StringType(),True),
                  StructField("age",IntegerType(),True),
                  StructField("designation",StringType(),True)])
dataframe=spark.createDataFrame(data=data,schema=datum)
dataframe.printSchema()
dataframe.show()


#dataframe created using structtype and struct_field

