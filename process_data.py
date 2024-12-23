""" This script processes the bulk download data from the Open Library project."""

import csv
import ctypes as ct
import os

INPUT_PATH = "/Users/charliealders/dev/data/unprocessed"
OUTPUT_PATH = "/Users/charliealders/dev/data/processed"

filesforprocessing = [
    # "ol_dump_works_2024-09-30.txt",
]

# See https://stackoverflow.com/a/54517228 for more info on this
csv.field_size_limit(int(ct.c_ulong(-1).value // 2))

for file in filesforprocessing:
    with open(os.path.join(OUTPUT_PATH, file), "w", newline="", encoding="utf-8") as csv_out:
        csvwriter = csv.writer(
            csv_out, delimiter="\t", quotechar="|", quoting=csv.QUOTE_MINIMAL
        )

        with open(os.path.join(INPUT_PATH, file), "r", encoding="utf-8") as csv_in:
            csvreader = csv.reader(csv_in, delimiter="\t")
            for row in csvreader:
                if len(row) > 4:
                    csvwriter.writerow(
                        [row[0], row[1], row[2], row[3], row[4]])
