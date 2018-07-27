from __future__ import print_function

# $example on$
from pyspark.ml.feature import Normalizer
from pyspark.ml.linalg import Vectors
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("NormalizerExample")\
        .getOrCreate()

    # $example on$
    dataFrame = spark.createDataFrame([
        (0, Vectors.dense([1.0, 0.5, -1.0]),),
        (1, Vectors.dense([2.0, 1.0, 1.0]),),
        (2, Vectors.dense([4.0, 10.0, 2.0]),)
    ], ["id", "features"])

    # Normalize each Vector using $L^1$ norm.
    normalizer = Normalizer(inputCol="features", outputCol="normFeatures", p=1.0)
    l1NormData = normalizer.transform(dataFrame)
    print("Normalized using L^1 norm")
    l1NormData.show()

    # Normalize each Vector using $L^\infty$ norm.
    lInfNormData = normalizer.transform(dataFrame, {normalizer.p: float("inf")})
    print("Normalized using L^inf norm")
    lInfNormData.show()
    # $example off$

    spark.stop()