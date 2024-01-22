import pyspark
from pyspark import SparkContext
sc =SparkContext()

inputfile = sc.textFile("input.txt")

inputfile

inputfile.toDebugString().decode()

result = inputfile\
    .flatMap(lambda x: x.split(" "))\
    .map(lambda x: (x,1))\
    .reduceByKey(lambda a,b: a+b)

result.cache()

result.collect()

result.unpersist()

!rm -rf /tmp/outputtest
result.saveAsTextFile("/tmp/outputtest")