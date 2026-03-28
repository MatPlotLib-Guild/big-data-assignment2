#!/usr/bin/env python3

import os
import re
import sys
from collections import Counter


TOKEN_PATTERN = re.compile(r"[^\W_]+", re.UNICODE)


def tokenize(text):
    return TOKEN_PATTERN.findall(text.lower())

input_file = os.environ.get("mapreduce_map_input_file", "")
if not input_file:
    input_file = os.environ.get("map_input_file", "")
if not input_file:
    sys.exit("Missing input file path in Hadoop streaming environment")

filename = input_file.rsplit("/", 1)[-1]
name = filename.rsplit(".", 1)[0] if "." in filename else filename
doc_id, title = name.split("_", 1)
title = title.replace("_", " ")

term_counts = Counter()
token_total = 0

for raw_line in sys.stdin:
    tokens = tokenize(raw_line)
    term_counts.update(tokens)
    token_total += len(tokens)

for term, count in sorted(term_counts.items()):
    print(f"POSTING\t{term}\t{doc_id}\t{title}\t{count}")

print(f"DOC\t__meta__\t{doc_id}\t{title}\t{token_total}")
