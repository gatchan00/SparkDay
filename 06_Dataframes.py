from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .getOrCreate()

empleados=spark.read.json("files/empleados.json")
empleados.show()

!rm -rf /tmp/empleados_csv
empleados.write.option("header",True).csv('/tmp/empleados_csv')

empleadoscsv=spark.read.options(header='True',inferSchema='True',delimiter=',')\
    .csv("files/empleados.csv")
empleadoscsv.show()

data = [[295, "South Bend", "Indiana", "IN", 101190, 112.9],\
        [132, "Callisto", "Indiana", "IN", 8790, 103.9]]
columns = ["rank", "city", "state", "code", "population", "price"]
schema="rank LONG, city STRING, state STRING, code STRING, population LONG, price DOUBLE"
df1 = spark.createDataFrame(data, schema=schema)
df1.show()

empleados_select = empleados.select("age","name")
empleados_select.show()

empleados_where = empleados.where(empleados["age"] > 23)
empleados_where.show()

!rm -rf /tmp/empleados_json
!rm -rf /tmp/empleados_csv
!rm -rf /tmp/empleados_avro
empleados_where.write\
    .format("json").save("/tmp/empleados_json")

empleados_where.write\
    .format("csv").save("/tmp/empleados_csv")

empleados_where.write\
    .format("avro").save("/tmp/empleados_avro")

#Save directly to HIVE
#empleados.write.mode("overwrite").saveAsTable("empleados")

from pyspark.sql.functions import lit
empleados2 = empleados.withColumn("country",lit("Portugal"))
empleados2.show()

from pyspark.sql import functions as F
empleados2.groupBy(["country"])\
.agg(F.avg("age").alias("AvgAge")).show()