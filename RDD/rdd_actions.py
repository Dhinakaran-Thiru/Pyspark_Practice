from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("performing actions").getOrCreate()
tex1=spark.sparkContext.parallelize([1,2,3,4,5,6,7,8,9,7,10])

collect_lines=tex1.collect()
print(collect_lines)
count_lines=tex1.count()
print(count_lines)
first=tex1.first()
print(first)
take=tex1.take(7)
print(take)

