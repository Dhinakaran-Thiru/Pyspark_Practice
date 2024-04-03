from pyspark.sql import SparkSession
from pyspark.sql.functions import upper,split
spark=SparkSession.builder.appName("creation of dataframe").getOrCreate()
dataframe=spark.read.option('header',True).csv(r"C:\Users\DHINAKARAN\Desktop\Database.csv")
dataframe.show()
dataframe.select("*",(upper(dataframe.DESIGNATION)).alias("upper_deisgnation")).show()
dataframe.select(upper(dataframe.NAME).alias("names in capital letter")).show()
#dataframe.select(dataframe.DESIGNATION,split(dataframe.DESIGNATION ,",").alias("different")).show()