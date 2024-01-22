from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession \
    .builder \
    .appName("Hands On example") \
    .getOrCreate()

flights=spark.read.options(header='True',inferSchema='True',delimiter=',')\
    .csv("files/521972167_T_ONTIME.csv")
flights.show()

flights.schema

import pyspark.sql.functions as F
top10airports = flights\
    .where(flights["DAY_OF_WEEK"] >5)\
    .groupBy(["origin"])\
    .agg(F.count(F.lit(1)).alias("total_departures"))\
    .orderBy(F.desc("total_departures"))\
    .limit(10)\
    .show()