from pyspark.sql import SparkSession, DataFrame
from obt import OBTContext
from time import time

#### Auto generating data
DATA_PATH = "/Users/charliealders/dev/data/processed/"

file_names = ["e_small.csv", "e_medium.csv", "e_large.csv"]

with open("big_epinions_results_reducers.csv", "w") as f:
    f.write("|S|,|T|,OUTPUT_COUNT,Time(s),Reducers\n")
    spark: SparkSession = SparkSession.builder.getOrCreate()
    reducers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 16, 24, 32, 48, 64, 96, 128]
    for file_name in file_names:
        print("Processing", file_name)
        for r in reducers:
            print("Using", r, "reducers...")
            spark.conf.set("spark.default.parallelism", r)
            df: DataFrame = spark.read.csv(f"{DATA_PATH}{file_name}")
            s = t = df.rdd
            obt = OBTContext(s, t, r)

            start = time()
            result = obt.one_bucket(1, 0, ">").count()
            end = time()
            f.write(f"{obt._card_s},{obt._card_t},{result},{end-start},{r}\n")
            print("Took", end-start, "seconds", "result:", result)
