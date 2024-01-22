import pyspark
from pyspark import SparkContext
sc =SparkContext()

textFile = sc.textFile("files/NCDCsample.txt")
rdd1 = textFile.map(lambda line: ( line.split('\t')[0],int(line.split('\t')[1])))
counts = rdd1.reduceByKey( lambda a,b: a if a<b else b )
counts.collect()

counts.saveAsTextFile("exercise01")