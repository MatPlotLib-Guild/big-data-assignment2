#!/usr/bin/env python3

import re
import sys
from collections import Counter


TOKEN_PATTERN = re.compile(r"[^\W_]+", re.UNICODE)


def tokenize(text):
    return TOKEN_PATTERN.findall(text.lower())

for raw_line in sys.stdin:
    fields = raw_line.rstrip("\n").split("\t", 2)
    if len(fields) != 3:
        continue

    doc_id, title, text = fields
    term_counts = Counter(tokenize(text))
    token_total = sum(term_counts.values())

    for term, count in sorted(term_counts.items()):
        print(f"POSTING\t{term}\t{doc_id}\t{title}\t{count}")

    print(f"DOC\t__meta__\t{doc_id}\t{title}\t{token_total}")
