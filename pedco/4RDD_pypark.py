
from pyspark.sql import SparkSession
   
def pruebaRDD():
    spark = SparkSession.builder.appName('ejemplo-de-rdd').getOrCreate()
    data = [1, 2, 3, 4, 5]
    distData = spark.sparkContext.parallelize(data)
    distDataMap=distData.map(lambda s: s+1)
    print(distDataMap.take(5))
    print(distDataMap.reduce(lambda a , b: a + b))

pruebaRDD()




