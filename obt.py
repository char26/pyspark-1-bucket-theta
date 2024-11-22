""" https://dl.acm.org/doi/10.1145/1989323.1989423 """
from random import randint
from pyspark.sql import Column
from math import sqrt, ceil
from typing import List, Optional

def obt():
    pass

class Mapper():
    def __init__(
            self,
            s_column: Column,
            card_s: int,
            t_column: Column,
            card_t: int,
            reducers: int
        ):
        """
        @param s_column: the column representing the smaller of the two columns
        @param card_s: the cardinality of the smaller column
        @param t_column: the column representing the larger of the two columns
        @param card_t: the cardinality of the larger column
        @param reducers: the number of reducers (for spark, this is default.parallelism)
        """
        # the column 's' is considered the smaller of the two columns by convention
        # so we first need to ensure the columns are in the correct order
        if card_s > card_t:
            s_column, t_column = t_column, s_column
            card_s, card_t = card_t, card_s
        self._s = s_column
        self._t = t_column
        self._card_s = card_s
        self._card_t = card_t
        self._reducers = reducers

        # Partition the matrix into reducer regions
        self._matrix_height = self._card_s
        self._matrix_width = self._card_t
        region_w = None
        region_h = None

        # Theorem 1, |S| and |T| are multiples of sqrt(|S||T|/r)
        if self._card_s % sqrt(self._card_s*self._card_t/self._reducers) == 0 \
            and self._card_t % sqrt(self._card_s*self._card_t/self._reducers) == 0:
            region_w = sqrt(self._card_s*self._card_t/self._reducers)
            region_h = sqrt(self._card_s*self._card_t/self._reducers)

        # Theorem 2, |S| < |T|/r
        elif self._card_s < self._card_t/self._reducers:
            region_w = self._card_s
            region_h = self._card_t/self._reducers

        # Theorem 3, |T|/r <= |S| <= |T|
        elif self._card_t/self._reducers <= self._card_s <= self._card_t:
            cs = self._card_s / sqrt(self._card_s*self._card_t/self._reducers)
            ct = self._card_t / sqrt(self._card_s*self._card_t/self._reducers)
            region_w = (1 + 1/min(cs, ct))*sqrt(self._card_s*self._card_t/self._reducers)
            region_h = (1 + 1/min(cs, ct))*sqrt(self._card_s*self._card_t/self._reducers)

        assert region_w is not None and region_h is not None, "Invalid region dimensions"
        self._region_w = region_w
        self._region_h = region_h
        print(f"Region width: {self._region_w}, Region height: {self._region_h}")

    def _get_regions(self, *, row: Optional[int] = None, col: Optional[int] = None) -> List[int]:
        """ Get the regions of the matrix that the row OR column belongs to
            @param row: row index
            @param col: column index
            @return: a list of regions that a row or column belongs to
        """
        # purposely not using "is not None" because I am starting columns and rows at 1
        assert row or col, "Must provide one of row or column index, starting at 1"
        assert not (row and col), "Cannot provide both row and column index"
        regions = []
        regions_per_col = int(self._card_s/self._region_h)
        regions_per_row = int(self._card_t/self._region_w)

        if row:
            column_region = ceil(row/self._region_h)
            regions_before = (column_region - 1) * regions_per_row
            start = regions_before + 1
            end = int(start + regions_per_row)
            for region in range(start, end):
                regions.append(region)
        elif col:
            row_region = ceil(col/self._region_w)
            regions_before = (row_region - 1) * regions_per_col
            start = regions_before + 1
            end = int(start + regions_per_col)
            for region in range(start, end):
                regions.append(region)

        assert len(regions) > 0, "No regions??"
        return regions

class Reducer():
    pass
