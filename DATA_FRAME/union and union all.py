'''from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
Spark=SparkSession.builder.appName("union and union all").getOrCreate()
data=[("dhina",56,"data engineer",123456),
      ("deepika",43,"junior",45432),
      ("deepa",56,"senior",56746)]
data1=[("deepa",56,"senior",56746),
       ("lakshmi",567,"blue colour",23455),
       ("chandana",999,"black colour",34535)]

Schema=StructType([StructField("NAME",StringType(),True),
                   StructField("AGE",IntegerType(),True),
                   StructField("DESIGNATION",StringType(),True),
                   StructField("Roll no",IntegerType(),True)
                  ])

df1=Spark.createDataFrame(data=data,schema=Schema)
df1.show(truncate=False)
df2=Spark.createDataFrame(data=data1,schema=Schema)
df2.show()
df1.union(df2).distinct().show()
#union() and union all() have same functionalities add the duplicate rows also
#by using distinct() we avoid duplicates'''
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# Prepare data
data = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4600),
    ("James", "Sales", 3000)
  ]
columns= ["employee_name", "department", "salary"]

# Create DataFrame
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.show(truncate=False)

# Using distinct()
distinctDF = df.distinct()
distinctDF.show(truncate=False)

# Using dropDuplicates()
dropDisDF = df.dropDuplicates(["department","salary"])
dropDisDF.show(truncate=False)

# Using dropDuplicates() on single column
dropDisDF = df.dropDuplicates(["salary"]).select("salary")
dropDisDF.show(truncate=False)
print(dropDisDF.collect())