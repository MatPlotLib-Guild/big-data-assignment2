#!/usr/bin/env python3

import math
import re
import sys
from collections import Counter

from pyspark.sql import SparkSession


TOKEN_PATTERN = re.compile(r"[^\W_]+", re.UNICODE)
KEYSPACE = "search_engine"
TOP_K = 10
K1 = 1.5
B = 0.75


def tokenize(text):
    return TOKEN_PATTERN.findall(text.lower())


def build_query_text(argv):
    if len(argv) > 1:
        return " ".join(argv[1:]).strip()
    return sys.stdin.read().strip()


def bm25_idf(document_count, document_frequency):
    numerator = document_count - document_frequency + 0.5
    denominator = document_frequency + 0.5
    return math.log(1.0 + (numerator / denominator))


def bm25_score(term_frequency, document_frequency, document_length, average_doc_length, document_count):
    numerator = term_frequency * (K1 + 1.0)
    denominator = term_frequency + K1 * (1.0 - B + B * (document_length / average_doc_length))
    return bm25_idf(document_count, document_frequency) * (numerator / denominator)


def load_corpus_stats(session):
    row = session.execute(
        """
        SELECT document_count, average_doc_length
        FROM corpus_stats
        WHERE stats_key = 'global'
        """
    ).one()
    return row.document_count, row.average_doc_length


def load_doc_stats(session):
    rows = session.execute("SELECT doc_id, title, doc_length FROM doc_stats")
    return {row.doc_id: (row.title, row.doc_length) for row in rows}


def load_term_data(session, query_terms):
    vocabulary_statement = session.prepare(
        "SELECT document_frequency FROM vocabulary WHERE term = ?"
    )
    postings_statement = session.prepare(
        """
        SELECT doc_id, title, term_frequency
        FROM postings_by_term
        WHERE term = ?
        """
    )

    term_frequencies = Counter(query_terms)
    term_payloads = []

    for term, query_frequency in term_frequencies.items():
        vocab_row = session.execute(vocabulary_statement, (term,)).one()
        if vocab_row is None:
            continue

        postings = []
        for row in session.execute(postings_statement, (term,)):
            postings.append((row.doc_id, row.term_frequency))

        term_payloads.append((query_frequency, vocab_row.document_frequency, postings))

    return term_payloads


def expand_term_payload(term_payload, doc_stats, average_doc_length, document_count):
    query_frequency, document_frequency, postings = term_payload
    rows = []

    for doc_id, term_frequency in postings:
        title, document_length = doc_stats[doc_id]
        score = bm25_score(
            term_frequency,
            document_frequency,
            document_length,
            average_doc_length,
            document_count,
        )
        rows.append((doc_id, (title, score * query_frequency)))

    return rows


def main(argv):
    query_text = build_query_text(argv)
    query_terms = tokenize(query_text)

    if not query_terms:
        print("Query is empty after tokenization", file=sys.stderr)
        return 1

    from cassandra.cluster import Cluster

    cluster = Cluster(["cassandra-server"])
    session = cluster.connect(KEYSPACE)

    document_count, average_doc_length = load_corpus_stats(session)
    doc_stats = load_doc_stats(session)
    term_payloads = load_term_data(session, query_terms)

    session.shutdown()
    cluster.shutdown()

    if not term_payloads:
        print("No indexed terms from the query were found.")
        return 0

    spark = SparkSession.builder.appName("search-query").getOrCreate()
    sc = spark.sparkContext
    doc_stats_broadcast = sc.broadcast(doc_stats)

    results = (
        sc.parallelize(term_payloads, len(term_payloads))
        .flatMap(
            lambda term_payload: expand_term_payload(
                term_payload,
                doc_stats_broadcast.value,
                average_doc_length,
                document_count,
            )
        )
        .reduceByKey(lambda left, right: (left[0], left[1] + right[1]))
        .map(lambda item: (item[0], item[1][0], item[1][1]))
        .takeOrdered(TOP_K, key=lambda item: -item[2])
    )

    print(f"Query: {query_text}")
    print("Top documents:")
    for rank, (doc_id, title, score) in enumerate(results, start=1):
        print(f"{rank}\t{doc_id}\t{score:.6f}\t{title}")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))