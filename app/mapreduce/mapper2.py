#!/usr/bin/env python3

import sys


for raw_line in sys.stdin:
    line = raw_line.rstrip("\n")
    if not line:
        continue

    fields = line.split("\t")
    if fields[0] != "POSTING":
        continue

    term = fields[1]
    print(f"VOCAB\t{term}\t1")
