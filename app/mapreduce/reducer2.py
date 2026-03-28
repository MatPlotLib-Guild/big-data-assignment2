#!/usr/bin/env python3

import sys


current_key = None
current_total = 0

for raw_line in sys.stdin:
    line = raw_line.rstrip("\n")
    if not line:
        continue

    fields = line.split("\t")
    key = tuple(fields[:-1])
    value = int(fields[-1])

    if current_key == key:
        current_total += value
        continue

    if current_key:
        print(f"{current_key[0]}\t{current_key[1]}\t{current_total}")

    current_key = key
    current_total = value

if current_key:
    print(f"{current_key[0]}\t{current_key[1]}\t{current_total}")
