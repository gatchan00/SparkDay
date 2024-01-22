import pyspark
from pyspark import SparkContext
sc =SparkContext()


b = sc.broadcast([1, 2, 3, 4, 5])
sc.parallelize([1, 2]).map(lambda x: x * b.value[-1]).collect()