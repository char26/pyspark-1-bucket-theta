from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from obt import OBTContext
from time import time

conf = SparkConf()
spark: SparkSession = SparkSession.builder.config(conf=conf).getOrCreate()

DATA_PATH = "/Users/charliealders/dev/data/processed/"

with open("broadband_results_reducers.csv", "w") as f:
    f.write("|S|,|T|,OUTPUT_COUNT,Time(s),Reducers\n")
    spark: SparkSession = SparkSession.builder.getOrCreate()
    file_name1 = "broadband_data_2020October.csv"
    file_name2 = "broadband_data_zipcode.csv"
    df1: DataFrame = spark.read.csv(f"{DATA_PATH}{file_name1}")
    df2: DataFrame = spark.read.csv(f"{DATA_PATH}{file_name2}")
    reducers = range(1, 100, 2)
    for r in reducers:
        print("Using", r, "reducers...")
        spark.conf.set("spark.default.parallelism", r)

        s = df1.rdd
        t = df2.rdd
        obt = OBTContext(s, t, r)

        start = time()
        result = obt.one_bucket(2, 1, "==").count()
        end = time()
        f.write(f"{obt._card_s},{obt._card_t},{result},{end-start},{r}\n")
        print("Took", end-start, "seconds")
