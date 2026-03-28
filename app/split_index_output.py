#!/usr/bin/env python3

import sys
from pathlib import Path


output_dir = Path(sys.argv[1])
output_dir.mkdir(parents=True, exist_ok=True)

postings_path = output_dir / "postings.tsv"
doc_stats_path = output_dir / "doc_stats.tsv"
corpus_stats_path = output_dir / "corpus_stats.tsv"

with postings_path.open("w", encoding="utf-8") as postings_file, \
        doc_stats_path.open("w", encoding="utf-8") as doc_stats_file, \
        corpus_stats_path.open("w", encoding="utf-8") as corpus_stats_file:
    for raw_line in sys.stdin:
        line = raw_line.rstrip("\n")
        if not line:
            continue

        record_type = line.split("\t", 1)[0]
        if record_type == "POSTING":
            postings_file.write(line + "\n")
        elif record_type == "DOC":
            doc_stats_file.write(line + "\n")
        elif record_type == "CORPUS":
            corpus_stats_file.write(line + "\n")
