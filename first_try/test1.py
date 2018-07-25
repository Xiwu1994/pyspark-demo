from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PythonWordCount")
sc = SparkContext(conf=conf)

links = sc.parallelize(["A","B","C","D"])
C = links.flatMap(lambda dest:(dest,1)).count()
D = links.map(lambda dest:(dest,1)).count()
print(C)
print(D)
c = links.flatMap(lambda dest:(dest,1)).collect()
d = links.map(lambda dest:(dest,1)).collect()
print(c)
print(d)