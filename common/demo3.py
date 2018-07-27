# coding:utf-8
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as func


spark = SparkSession.builder.master("local").appName("demo3").getOrCreate()
sc = spark.sparkContext

# Load a text file and convert each line to a Row.
lines = sc.textFile("data/demo2.txt")
parts = lines.map(lambda l: l.split(" ")) \
    .map(lambda p: Row(exam_type=p[0], student_id=p[1], score=p[2])) \
    .toDF()

parts.persist()
parts.show()
parts.printSchema()


# 1、考试科目为math的最高分
parts.where("exam_type = 'math'") \
    .agg(func.max("score")).show()

# 2、分数>60分的占比
print parts.where("score > 60").count() * 1.0 / parts.count()

# 3、求出每一种科目的最高分
exam_type_max_score = parts.groupBy("exam_type").agg(func.max("score").alias("max_score"))
exam_type_max_score.persist()
exam_type_max_score.show()
exam_type_max_score.printSchema()


# 4、求出每一种科目的最高分和对应的学生
exam_type_max_score \
    .join(parts, on=[exam_type_max_score["exam_type"] == parts["exam_type"]]) \
    .where("max_score = score").show()


# 用sql的方式处理
parts.createOrReplaceTempView("tmp_table")
spark.sql("select max(score) from tmp_table where exam_type='math'").show()
spark.sql("select sum(if(score>60, 1, 0)) / count(1) from tmp_table").show()
spark.sql("select exam_type, max(score) from tmp_table group by exam_type").show()
spark.sql("select t1.exam_type, t1.max_score, t2.student_id from (select exam_type,max(score) as max_score from tmp_table group by exam_type) t1 left join tmp_table t2 on t1.exam_type = t2.exam_type and t1.max_score = t2.score").show()
