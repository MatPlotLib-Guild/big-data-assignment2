#!/usr/bin/env python3

import sys


def emit_record(key, total):
    if not key:
        return 0, 0

    record_type = key[0]
    if record_type == "DOC":
        doc_id = key[2]
        title = key[3]
        print(f"DOC\t{doc_id}\t{title}\t{total}")
        return 1, total

    if record_type == "POSTING":
        term = key[1]
        doc_id = key[2]
        title = key[3]
        print(f"POSTING\t{term}\t{doc_id}\t{title}\t{total}")

    return 0, 0


current_key = None
current_total = 0
document_count = 0
total_doc_length = 0

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

    docs_added, length_added = emit_record(current_key, current_total)
    document_count += docs_added
    total_doc_length += length_added

    current_key = key
    current_total = value

docs_added, length_added = emit_record(current_key, current_total)
document_count += docs_added
total_doc_length += length_added

print(f"CORPUS\t{document_count}\t{total_doc_length}")