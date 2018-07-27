from __future__ import print_function

# $example on$
from pyspark.ml.feature import StandardScaler
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("StandardScalerExample")\
        .getOrCreate()

    # $example on$
    dataFrame = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures",
                            withStd=True, withMean=False)

    # Compute summary statistics by fitting the StandardScaler
    scalerModel = scaler.fit(dataFrame)

    # Normalize each feature to have unit standard deviation.
    scaledData = scalerModel.transform(dataFrame)
    scaledData.show()
    # $example off$

    spark.stop()