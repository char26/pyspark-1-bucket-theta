from pyspark.sql import SparkSession, DataFrame, Column
from pyspark import SparkConf
from obt import Matrix

# conf = SparkConf()

# conf.set("spark.default.parallelism", 200)

# spark = SparkSession.builder.config(conf=conf).getOrCreate()

# df: DataFrame = spark.read.option("header", True).csv("./epinions_small.csv")
# card_s = df.count()
# card_t = df.count()


matrix = Matrix(6, 6, 36)
print(matrix.get_regions(col=4))

# df.printSchema()
# print(df.count()**2)
