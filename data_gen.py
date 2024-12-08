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

DATA_PATH = "/Users/charliealders/dev/data/processed/"
file_names = [
    "e_small.csv",
    "e_medium.csv",
    "e_large.csv",
    "e_xl.csv",
    "e_xxl.csv",
]
join_conditions = [
    (lambda s, t: s[1] == t[0], "=="),
    (lambda s, t: s[1] < t[0], "<"),
    (lambda s, t: s[1] > t[0], ">"),
    (lambda s, t: s[1] <= t[0], "<="),
    (lambda s, t: s[1] >= t[0], ">="),
]

with open("epinions_results.csv", "w") as f:
    f.write("|S|,|T|,OUTPUT_COUNT,Time(s),Join Condition\n")
    for file_name in file_names:
        df: DataFrame = spark.read.csv(f"{DATA_PATH}{file_name}")
        s = t = df.rdd
        obt = OBTContext(s, t, reducers)
        print("Processing", file_name, "of size", obt._card_s)
        for join_condition, join_str in join_conditions:
            print("Joining", join_str)
            start = time()
            result = obt.one_bucket(join_condition).count()
            end = time()
            f.write(f"{obt._card_s},{obt._card_t},{result},{end-start},{join_str}\n")
            print("Took", end-start, "seconds")
        del df
        del s
        del t


