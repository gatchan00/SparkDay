from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession\
    .builder\
    .appName("PythonPi")\
    .getOrCreate()

df=spark.sql("show databases")
df.show()

spark.sql("show tables").show()

df2=spark.sql("select * from default.curated")
df2.show()