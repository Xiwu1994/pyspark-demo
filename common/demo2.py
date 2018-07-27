# coding:utf-8
from pyspark.sql import SparkSession


# 转换算子 http://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations
# 行动算子 http://spark.apache.org/docs/latest/rdd-programming-guide.html#actions

spark = SparkSession.builder.master("local").appName("demo2").getOrCreate()
sc = spark.sparkContext
lines = sc.textFile("data/demo2.txt")
lines.persist()

# exam_type  student_id  score
# math 101 66
# math 102 80
# math 103 95
# english 101 59
# english 102 98
# english 103 65


# 1、考试科目为math的最高分
def split_line(line):
    x = line.split(" ")
    return (x[0], x[1], int(x[2]))


print lines.map(split_line).filter(lambda x: x[0] == "math").max(lambda x: x[2])

# map 转换算子:  将原来 RDD 的每个数据项通过 map 中的用户自定义函数 f 映射转变为一个新的元素
# filter 转换算子:  filter函数功能是对元素进行过滤，对每个元素应用f函数，返回值为true的元素在RDD中保留，返回值为false的元素将被过滤掉。
# max 行动算子: 对于输入的元素 找到最大值

# 2、分数>60分的占比
fenzi = lines.map(split_line).filter(lambda x: x[2] > 60).count()
fenmu = lines.count()
print fenzi * 1.0 / fenmu

# count 行动算子: 找出多少个 输出的记录

# 3、求出每一种科目的最高分
def exam_type_score(line):
    x = line.split(" ")
    return (x[0], int(x[2]))


print lines.map(exam_type_score).reduceByKey(lambda x, y: max(x, y)).collect()

# reduceByKey 转换算子:  只对 key-values pairs
#   输入两个参数 x表示上一行的结果值, y表示当前值,
#   对应的函数返回值代表下一行的结果值，也就是下一行的x

# collect 行动算子: collect 将分布式的RDD 返回为一个单机的数组， 类似于 print


# 4、求出每一种科目的最高分和对应的学生
# 提示: 可以用 map reduceByKey 算子完成
#      map里函数返回 和第三题不一样，因为要找到对应的学生。 key-value 的value不局限于一个字段
















def split_exam(line):
    x = line.split(" ")
    return (x[0], (x[1], int(x[2])))


def max_value(x, y):
    if x[1] >= y[1]:
        return x
    else:
        return y


print lines.map(split_exam).reduceByKey(max_value).collect()

