from pyspark import RDD
from matrix import Matrix
from random import randint
from sortedcontainers import SortedDict


class OBTContext():
    def __init__(self, s: RDD, t: RDD, reducers: int):
        self._s = s
        self._t = t
        self._card_s = s.count()
        self._card_t = t.count()
        self._matrix = Matrix(self._card_s, self._card_t, reducers)

    def one_bucket(
            self,
            s_join_index: int = 0,
            t_join_index: int = 0,
            join_cond: str = "=="
        ) -> RDD:
        s = self._s
        t = self._t
        card_s = self._card_s
        card_t = self._card_t
        matrix = self._matrix

        def _s_mapper(row: tuple):
            matrix_row = randint(1, card_s)
            regions = matrix.get_row_regions(matrix_row)
            for region in regions:
                yield (region, row + tuple('S'))

        def _t_mapper(row: tuple):
            matrix_col = randint(1, card_t)
            regions = matrix.get_col_regions(matrix_col)
            for region in regions:
                yield (region, row + tuple('T'))

        def _join(
                s_tuples: set,
                t_tuples: SortedDict,
                s_join_index: int = 0,
                join_cond: str = "=="
            ):
            results = []
            for s in s_tuples:
                inclusive = len(join_cond) == 2 and join_cond[1] == "="
                if join_cond == "==": # equi join
                    for t in t_tuples.get(s[s_join_index], []):
                        results.append((s, t))
                elif join_cond[0] == "<": # lt/lte join
                    iterator = t_tuples.irange(maximum=s[s_join_index], inclusive=(True, inclusive))
                    for t in iterator:
                        for row in t_tuples[t]:
                            results.append((s, row))
                elif join_cond[0] == ">": # gt/gte join
                    iterator = t_tuples.irange(minimum=s[s_join_index], inclusive=(inclusive, True))
                    for t in iterator:
                        for row in t_tuples[t]:
                            results.append((s, row))

            return results


        def _reducer(
                    _,
                    values: list,
                    s_join_index: int = 0,
                    t_join_index: int = 0,
                    join_cond: str = "=="
                ):
            s_tuples = set()
            t_tuples = SortedDict()

            for value in values:
                row = value[:-1]
                origin = value[-1]

                if origin == 'S':
                    s_tuples.add(row)
                else:
                    key = row[t_join_index]
                    if key not in t_tuples:
                        t_tuples[key] = []
                    t_tuples[key].append(row)
            join_result = _join(s_tuples, t_tuples, s_join_index, join_cond)

            return join_result

        total_regions = matrix.get_total_regions()
        s = s.flatMap(_s_mapper)
        t = t.flatMap(_t_mapper)
        union = s.union(t)
        result = union.groupByKey(total_regions).flatMap(lambda x: _reducer(x[0], list(x[1]), s_join_index, t_join_index, join_cond))
        return result





