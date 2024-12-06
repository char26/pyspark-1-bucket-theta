from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from obt import OBTContext
from time import time

reducers = 128

conf = SparkConf()
conf.set("spark.default.parallelism", reducers)
conf.set("spark.executor.memory", "4g")
conf.set("spark.driver.memory", "4g")
spark: SparkSession = SparkSession.builder.config(conf=conf).getOrCreate()
df: DataFrame = spark.read.option("header", False).csv("./data/epinions_small.csv")
s = df.rdd
t = df.rdd

context = OBTContext(s, t, reducers)

start = time()
result = context.one_bucket(lambda s, t: s[1] == t[0])
print("Count:", result.count(), "Time:", time() - start)

