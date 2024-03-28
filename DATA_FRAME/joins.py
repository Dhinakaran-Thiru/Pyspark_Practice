from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("joints").getOrCreate()
data1=[("david",123,"blue shirt"),
       ("lakshmi",567,"blue colour"),
       ("chandana",999,"black colour")]
data2=[("lakshmi",567,"blue colour"),("gowtham",547,"white colour"),("Ashok",52,"number one black colour")]

df1=spark.createDataFrame(data=data1,schema=(["name","age","colour"]))
df2=spark.createDataFrame(data=data2,schema=(["name","Roll no","colourly"]))

df3=df1.join(df2, "name", "left")
df3.show()

df4=df1.join(df2, "name", "right")
df4.show()

df5=df1.join(df2, "name", "inner")
df5.show()

df6=df1.join(df2, "name", "cross")
df6.show()

df7=df1.join(df2, "name", "cross")
df7.show()

df8=df1.join(df2, "name", "anti")
df8.show()


