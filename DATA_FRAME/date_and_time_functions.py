from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,current_date,to_date,year,month,dayofmonth,trunc,date_format,datediff,months_between
Spark=SparkSession.builder.appName("date and time functioins").getOrCreate()
data=[["1","2020-02-01"],["2","2021-02-04"],["3","2022-03-09"]]
df=Spark.createDataFrame(data,['id','input'])
df.show()
df.select(current_date().alias("current_date")).show(1)
df.select(col("input"),to_date(col("input")).alias("string to date")).show()
df.select(col("input"), date_format(col("input"), 'dd-MM-yyyy').alias("date_format")).show()
df.select(col("input"),datediff(current_date(),col("input")).alias("date_difference")).show()
df.select(col("input"),months_between(current_date(),col("input")).alias("months_between")).show()
df.withColumn("truncate",trunc("input", "day")).show()
df.select(col("input"),trunc(col("input")),"Month").show()