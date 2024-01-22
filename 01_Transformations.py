import pyspark
from pyspark import SparkContext
sc =SparkContext()

movies = sc.parallelize( ["Frozen, 2013",\
                               "Toy Story, 1995",\
                               "WALL-E, 2008","Despicable Me,\
                               2010", "Shrek, 2001",\
                               "The Lego Movie, 2014",\
                               "Alice in Wonderland,2010"] )
movies.collect()

filtro = (lambda m: int(m.split(",")[-1].strip()) == 2010)
movies2010 = movies.filter(filtro)

movies2010.collect()

familyMovies = movies2010.map(lambda m:(m, {"Family","Animation"}))
familyMovies.collect()

mapMovies = movies.map(lambda m: (m.split(",")[0], m.split(",")[-1].strip()))
mapMovies.collect()

familyGenres = familyMovies.flatMap(lambda tuple: tuple[1]).distinct()
familyGenres.collect()

pairs = familyGenres.cartesian(familyGenres).filter(lambda tuple: tuple[0] != tuple[1])
pairs.collect()

#Extra
rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 2),("a", 9)])
seqFunc = (lambda acc, value: (acc[0] + str(value) , acc[1] + value))
combFunc = (lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]))
sorted(rdd.aggregateByKey(("", 0), seqFunc, combFunc).collect())