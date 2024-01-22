from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL another example") \
    .getOrCreate()

flights=spark.read.options(header='True',inferSchema='True',delimiter=',')\
    .csv("files/521972167_T_ONTIME.csv")
flights.createOrReplaceTempView("flights")
totalFlights = spark.sql("SELECT count(*) AS total FROM flights")
totalFlights.show()

flights.createOrReplaceTempView("flights")
totalFlights = spark.sql("SELECT count(*) AS total FROM flights")
totalFlights.show()

# Top 10 performances airports on weekends
top10airports = spark.sql("SELECT origin, count(*) AS total_departures " +
  "FROM flights WHERE DAY_OF_WEEK > 5 GROUP BY origin " +
  "ORDER BY total_departures DESC LIMIT 10")
top10airports.show()

# Top 5 delayed airports
top5delayed = spark.sql("SELECT origin, count(1) as cnt " +
  "FROM flights WHERE ARR_DELAY >= 15 GROUP BY origin " +
  "ORDER BY cnt DESC LIMIT 5")
top5delayed.show()