""" https://dl.acm.org/doi/10.1145/1989323.1989423 """
from random import randint
from math import sqrt, ceil, floor
from typing import List, Optional

def obt():
    pass

class Matrix():
    """
    Represents the matrix used in 1-Bucket-Theta. Note that there is no actual matrix,
    but we are simply using the concept of a matrix to represent the data.
    """
    def __init__(
            self,
            card_s: int,
            card_t: int,
            reducers: int,
            debug: Optional[bool] = False
        ):
        """
        @param card_s: the cardinality of the smaller column
        @param card_t: the cardinality of the larger column
        @param reducers: the number of reducers (for spark, this is default.parallelism)
        """
        # the column 's' is considered the smaller of the two columns by convention
        # so we first need to ensure the columns are in the correct order
        if card_s > card_t:
            card_s, card_t = card_t, card_s
        self._card_s = card_s
        self._card_t = card_t
        self._reducers = reducers
        self._debug = debug

        sqrt_output = sqrt(self._card_s*self._card_t/self._reducers)

        # Partition the matrix into reducer regions
        self._matrix_height = self._card_s
        self._matrix_width = self._card_t
        region_w = region_h = None
        cs = ct = None

        # Theorem 1, |S| and |T| are multiples of sqrt(|S||T|/r)
        if self._card_s % sqrt_output == 0 \
            and self._card_t % sqrt_output == 0:
            region_w = region_h = sqrt_output
            cs = int(card_s / sqrt_output)
            ct = int(card_t / sqrt_output)

        # Theorem 2, |S| < |T|/r
        elif self._card_s < self._card_t/self._reducers:
            if self._debug:
                print("Theorem 2")
            region_w = self._card_t/self._reducers
            region_h = self._card_s
            cs = 1
            ct = self._reducers

        # Theorem 3, |T|/r <= |S| <= |T|
        elif self._card_t/self._reducers <= self._card_s <= self._card_t:
            cs = int(self._card_s / sqrt_output)
            ct = int(self._card_t / sqrt_output)
            if self._debug:
                print("Theorem 3")
                print(cs, ct)
            region_w = (1 + 1/min(cs, ct))*sqrt_output
            region_h = (1 + 1/min(cs, ct))*sqrt_output


        assert region_w is not None and region_h is not None, "Invalid region dimensions"
        assert cs is not None and ct is not None, "Invalid c_s and c_t"
        # NOTE: using ceil here is actually not very optimal, but it is the only
        # way I could get consistent results in the short time frame of this project
        self._region_w = ceil(region_w)
        self._region_h = ceil(region_h)
        self._cs = cs
        self._ct = ct
        if self._debug:
            print(f"Region width: {self._region_w}, Region height: {self._region_h}")

    def get_row_regions(self, row: int) -> List[int]:
        assert row <= self._card_s, "Row out of bounds"
        regions_per_row = int(self._card_t/self._region_w)
        column_region = ceil(row/self._region_h)
        regions_before = (column_region - 1) * regions_per_row
        start = regions_before + 1
        end = int(start + regions_per_row)
        if self._debug:
            print(f"Start: {start}, " + f"End: {end}")
        return(list(range(start, end)))

    def get_col_regions(self, col: int) -> List[int]:
        assert col <= self._card_t, "Column out of bounds"
        start = ceil(col/self._region_w)
        end = self._ct * self._cs + 1
        if self._debug:
            print(f"Start: {start}, End: {end}")
        return(list(range(start, end, self._ct)))
