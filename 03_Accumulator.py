import pyspark
from pyspark import SparkContext
sc =SparkContext()

accu=sc.accumulator(0)
sc.parallelize([1,2,3,4]).foreach(lambda x: accu.add(x) )

accu.value