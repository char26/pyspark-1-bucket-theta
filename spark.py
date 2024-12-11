from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from obt import OBTContext
from time import time

reducers = 120

conf = SparkConf()
conf.set("spark.default.parallelism", reducers)
conf.set("spark.executor.memory", "4g")
conf.set("spark.driver.memory", "4g")
spark: SparkSession = SparkSession.builder.config(conf=conf).getOrCreate()

DATA_PATH = "/Users/charliealders/dev/data/processed/"

file_name = "e_xl.csv"
df: DataFrame = spark.read.csv(f"{DATA_PATH}{file_name}")
s = t = df.rdd
obt = OBTContext(s, t, reducers)
print("Processing", file_name, "of size", obt._card_s)
start = time()
result = obt.one_bucket(1, 0, "<").count()
end = time()
print("Took", end-start, "seconds", "Count:", result)



