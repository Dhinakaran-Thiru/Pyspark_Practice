from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import col,lit
spark=SparkSession.builder.appName("df creation").getOrCreate()
data=[('dhina','karan',12,'student'),
      ('sachin','tendulkhar',56,'data engineer'),
      ('deepa','kamal',28,'teacher'),
      ('deepika','',24,'beautician')]
schema=["firstname","lastname","age","designation"]
df=spark.createDataFrame(data=data,schema=schema)
df.show()
df.printSchema()
df1=df.withColumn(colName='age', col=col('age').cast('Integer'))#changing the datatype using the with column
df2=df.withColumn(colName='FirstName',col=col('firstname').cast("Integer"))#changing the datatype()
df1.show()
df2.show()
df1.printSchema()
df2.printSchema()
df3=df1.withColumn('age',col('age')*2).show()
#if we are using same column name means one column only come different column means two column will come
df1.withColumn('Country',lit('india')).show()
#lit is used here what menans by adding new column in a dataframe values are inserted like in a using lit only
df2.withColumn("duplicated_column",col('lastname')).show()





