from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from obt import OBTContext
from time import time

reducers = 121

conf = SparkConf()
conf.set("spark.default.parallelism", reducers)
conf.set("spark.executor.memory", "4g")
conf.set("spark.driver.memory", "4g")
spark: SparkSession = SparkSession.builder.config(conf=conf).getOrCreate()

DATA_PATH = "/Users/charliealders/dev/data/processed/"
file_names = [
    "e_small.csv",
    "e_medium.csv",
    "e_large.csv",
    "e_xl.csv",
    "e_xxl.csv",
    "e_xxxl.csv"
]
join_conditions = [
    (lambda s, t: s[1] == t[0], "=="),
    (lambda s, t: s[1] < t[0], "<"),
    (lambda s, t: s[1] > t[0], ">"),
    (lambda s, t: s[1] <= t[0], "<="),
    (lambda s, t: s[1] >= t[0], ">="),
]

file_name = "e_small.csv"
df: DataFrame = spark.read.csv(f"{DATA_PATH}{file_name}")
s = t = df.rdd
obt = OBTContext(s, t, reducers)
print("Processing", file_name, "of size", obt._card_s)
start = time()
result = obt.one_bucket(lambda s, t: s[1] == t[0]).count()
end = time()
print("Took", end-start, "seconds", "Count:", result)



