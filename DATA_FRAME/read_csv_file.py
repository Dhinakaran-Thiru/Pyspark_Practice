'''from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.functions import col


spark=SparkSession.builder.appName("read csv file").getOrCreate()
list1=spark.read.option('header',True).csv('Book1.csv')
print(list1.collect())
list1.show()


list2=list1.take(3)
for i in list2:
    print(i)
list1.printSchema()


list3=list1.count()
print(list3)


list5=list1.select('name')#select the particular column
list5.show()

#filter
list6=list1.filter(list1.age>100)
list6.show()

#when condition for when operation we should import (pysaprk.sql.functions import when _
list7=list1.withColumn("age group",when (list1.age<50, 'young').when (list1.age>50, 'old'))
list7.show()

#where
list8=list1.where(list1.age>50)
list8.show()

#The like() function is not directly available in PySpark DataFrames. However, you can achieve similar functionality using
# string manipulation functions such as startswith(), endswith(), or contains() from the pyspark.sql.functions module.

list9=list1.filter(col("name").startswith('t'))
list9.show() #we also just import col functions

list10=list1.sort(list1.name,Ascending=True)
list10.show()

list11=list1.describe()
list11.show()

list12=list3.columns() #it is not working
print(list12)'''

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import col
spark=SparkSession.builder.appName("new creration").getOrCreate()

data=[("dhinakaran",1,"science","good attitude",100),
      ("deepa",2,"maths","bad attitude",200),
      ("deepika",3,"social","better attitude",500)]
sch=StructType([StructField("name",StringType(),True),
               StructField("roll no",IntegerType(),True),
               StructField("subject",StringType(),True),
               StructField("attitude",StringType(),True),
               StructField("marks",IntegerType(),True)])

dataframe=spark.createDataFrame(data=data,schema=sch)
dataframe.show()
dataframe.printSchema()

data1=dataframe.collect()
data2=dataframe.take(1)
for i in data2:
    print(i)

dataframe.printSchema()
dataframe.count()


data4=dataframe.select("attitude")
data4.show()

data5=dataframe.filter(dataframe.marks>100)
data5.show()

data6=dataframe.filter(col("attitude").contains('atti'))
data6.show()
