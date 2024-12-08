from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from obt import OBTContext
from time import time

#### Auto generating data
DATA_PATH = "/Users/charliealders/dev/data/processed/"
file_names = [
    "e_small.csv",
    "e_medium.csv",
    "e_large.csv",
]

join_conditions = [
    (lambda s, t: s[1] < t[0], "<"),
]

with open("epinions_results_reducers.csv", "w") as f:
    f.write("|S|,|T|,OUTPUT_COUNT,Time(s),Reducers\n")
    spark: SparkSession = SparkSession.builder.getOrCreate()
    for file_name in file_names:
        reducers = [9, 16, 25, 36, 49, 64, 81, 100, 121, 200]
        print("Processing", file_name)
        for r in reducers:
            print("Using", r, "reducers...")
            spark.conf.set("spark.default.parallelism", r)
            df: DataFrame = spark.read.csv(f"{DATA_PATH}{file_name}")
            s = t = df.rdd
            obt = OBTContext(s, t, r)

            for join_condition, join_str in join_conditions:
                start = time()
                result = obt.one_bucket(join_condition).count()
                end = time()
                f.write(f"{obt._card_s},{obt._card_t},{result},{end-start},{r}\n")
                print("Took", end-start, "seconds")
