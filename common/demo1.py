# coding:utf-8

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

# step 1: create SparkContext
# The first thing a Spark program must do is to create a SparkContext object,
# which tells Spark how to access a cluster.

# 方法一 (老版本)
# To create a SparkContext you first need to build a SparkConf object
# that contains information about your application.

# conf = SparkConf().setAppName("demo1").setMaster("local")
# sc = SparkContext(conf=conf)

# 方法二 (新版本, 建议)
spark = SparkSession.builder.master("local").appName("demo1").getOrCreate()
sc = spark.sparkContext


# step 2: create RDDs
# Spark revolves around the concept of a resilient distributed dataset (RDD),
# which is a fault-tolerant collection of elements that can be operated on in parallel.
# There are two ways to create RDDs:
# 1、parallelizing an existing collection in your driver program,
# 2、referencing a dataset in an external storage system,
#    such as a shared filesystem, HDFS, HBase, or any data source offering a Hadoop InputFormat.

# 方法一 通过parallelize创建rdd
data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)

# 方法二 通过外部数据集 本地文件 or HDFS
lines = sc.textFile("data/demo1.txt")
lines.persist()


# step3 : rdd operations
# RDDs support two types of operations:
# 1、transformations, which create a new dataset from an existing one,
# 转换算子：这种变换并不触发提交作业，完成作业中间过程处理。
# 2、actions, which return a value to the driver program after running a computation on the dataset.
# 行动算子会触发 SparkContext 提交 Job 作业

# transformation操作
lineLengths = lines.map(lambda s: len(s))
# print lineLengths.collect()

# action操作
totalLength = lineLengths.reduce(lambda a, b: a + b)
# print totalLength


# step4: 传递函数
# 1、Lambda expressions, for simple functions that can be written as an expression.
# 2、Local defs inside the function calling into Spark, for longer code.

def myFunc(s):
    words = s.split(" ")
    return len(words)


lineSplitLen = lines.map(myFunc)


# step5: Working with Key-Value Pairs
# Key-Value 这样的数据类型 在平常的操作中很常见，类似的sql操作中的 join , group by , order by
# word count 例子中用到 Key-Value 数据类型

wordCounts = lines.flatMap(lambda s: s.split(" ")) \
    .map(lambda s: (s, 1)) \
    .reduceByKey(lambda a, b: a + b)


print wordCounts.collect()
print wordCounts.sample().collect()

