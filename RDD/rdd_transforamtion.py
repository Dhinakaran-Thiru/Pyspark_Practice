import pyspark
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("creating transforamtion actions").getOrCreate()
file1=spark.sparkContext.textFile(r"C:\Users\DHINAKARAN\Desktop\New Text Document.txt")
print(file1.collect())
sc=spark.sparkContext
#map_function
x_map=sc.parallelize([1,3,5,6,8,89,6,4,4])
y_map=x_map.map(lambda x:(x,x**2))
print(x_map.collect())
print(y_map.collect())

#flat_map function
x_flatmap=sc.parallelize([4,6,6,7,4,5,6,4])
y_flatmap=x_flatmap.map(lambda x:(x,x**2,100*x))
print(x_flatmap.collect())
print(y_flatmap.collect())

#filter function
x_filter=sc.parallelize([1,4,3,5,7,3,6,7,3,6,35,56,68,45,67,56])
y_filter=x_filter.filter(lambda x:(x%2==0))
print(x_filter.collect())
print(y_filter.collect())

#map_partition
data=range(20)
x=sc.parallelize(data,3)
def f(iterator):
    yield sum(iterator)
y=x.mapPartitions(f)
print(x.glom().collect())
print(y.glom().collect())

##map_partition_with_index
data=[1,23,5,34,45,34,4,242,4,42,242424,4242,4]
r=sc.parallelize(data,4)
def f(partitionIndex,iterator):
    yield (partitionIndex,sum(iterator))
s=r.mapPartitionsWithIndex(f)
print(r.glom().collect())
print(s.glom().collect())

#sample--fraction of fraction data
#-true it is repaeated
#-fale it is not repeated #0-empty,1 full data,0.5 fifty percent data it will come
de=sc.parallelize([1,2,3,4,5,6,7,8,9])
re=de.sample(True,1)
print(de.collect())
print(re.collect())

dig1=sc.parallelize([2,23,34,34,34,43,35,65,57,6])
dig2=sc.parallelize([1,3,5,6,7,23,2,56,34,8,5,9,234])
re1=dig1.union(dig2)
print(re1.collect())
re2=dig1.intersection(dig2)
print(re2.collect())

#distinct
re3=sc.parallelize([1,2,3,4,5,6,1,2,3,4,5])
re4=re3.distinct()
print(re4.collect())

#groupby
datas=(['apple','apricot','orange','dhinakaran','animal','cat'])
datum=sc.parallelize(datas)
distinced_group_by=datum.groupBy(lambda x:x[0])
for key,values in distinced_group_by.collect():
    print(key,list(values))
