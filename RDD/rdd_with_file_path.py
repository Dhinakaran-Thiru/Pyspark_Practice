import pyspark
from pyspark.sql import SparkSession
spark=(SparkSession.builder\
    .appName("creating files")\
       .getOrCreate())
data=spark.sparkContext.textFile(r"C:\Users\DHINAKARAN\Desktop\New Text Document.txt")
print(data.collect())
 
