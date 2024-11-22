from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from obt import Mapper

# conf = SparkConf()

# conf.set("spark.default.parallelism", 200)

# spark = SparkSession.builder.config(conf=conf).getOrCreate()

# df: DataFrame = spark.read.option("header", True).csv("./epinions_small.csv")
# s = df["fromNodeId"]
# t = df["toNodeId"]
mapper = Mapper(None, 6, None, 6, 36)
print(mapper._get_regions(row=6))

# df.printSchema()
# print(df.count()**2)
