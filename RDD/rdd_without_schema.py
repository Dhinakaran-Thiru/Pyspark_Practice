from pyspark.sql import SparkSession
spark=SparkSession.builder\
.appName("creating rdd without schema").getOrCreate()

data=[("dhinakaran",1),("deepika",2),("deepa",3)]
rdd=spark.sparkContext.parallelize(data)
print(rdd.collect())

'''from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('practice').getOrCreate()
rdd=spark.sparkContext.textFile(r"C:\Users\DHINAKARAN\Desktop\names.csv")'''
