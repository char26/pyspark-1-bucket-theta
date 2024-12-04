from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from obt import Matrix
from random import randint

reducers = 9 # TODO: ONLY 1, 9, and 36 GET THE RIGHT ANSWER.......

conf = SparkConf()
conf.set("spark.default.parallelism", reducers)
spark: SparkSession = SparkSession.builder.config(conf=conf).getOrCreate()
df: DataFrame = spark.read.option("header", True).csv("./epinions.csv")
s = df.rdd
t = df.rdd

card_s = s.count()
card_t = t.count()

# print(len(s.collect()))

matrix = Matrix(card_s, card_t, reducers)

def s_mapper(row):
    from_node_id = row[0]
    to_node_id = row[1]
    matrix_row = randint(1, card_s)
    regions = matrix.get_row_regions(matrix_row)
    for region in regions:
        yield (region, (from_node_id, to_node_id, 'S'))


def t_mapper(row):
    from_node_id = row[0]
    to_node_id = row[1]
    matrix_col = randint(1, card_t)
    regions = matrix.get_col_regions(matrix_col)
    for region in regions:
        yield (region, (from_node_id, to_node_id, 'T'))

s = s.flatMap(s_mapper)
t = t.flatMap(t_mapper)
union = s.union(t)


def reducer(_, values: list):
    from collections import defaultdict

    s_tuples = set()
    t_tuples = defaultdict(list)

    for value in values:
        from_node_id = value[0]
        to_node_id = value[1]
        origin = value[2]

        if origin == 'S':
            s_tuples.add((from_node_id, to_node_id))
        else:
            t_tuples[from_node_id].append(to_node_id)

    join_result = []

    for s_from_node_id, s_to_node_id in s_tuples:
        if s_to_node_id in t_tuples:
            for t_to_node_id in t_tuples[s_to_node_id]:
                join_result.append((s_from_node_id, s_to_node_id, t_to_node_id))

    return join_result

print(union.groupByKey().flatMap(lambda x: reducer(x[0], list(x[1]))).count())
