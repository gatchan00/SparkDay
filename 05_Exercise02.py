import pyspark
from pyspark import SparkContext
sc =SparkContext()

textFile = sc.textFile("files/NCDCsample.txt")
rdd1 = textFile.map(lambda line: ( line.split('\t')[0],int(line.split('\t')[1])))

seqFunc = (lambda acc, value: (acc[0] + value , acc[1] + 1))
combFunc = (lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]))
avgTempsRaw = rdd1.aggregateByKey((0, 0), seqFunc, combFunc)

avgTemps = avgTempsRaw.map(lambda tuple: (tuple[0], tuple[1][0] / tuple[1][1]) )
avgTemps.collect()