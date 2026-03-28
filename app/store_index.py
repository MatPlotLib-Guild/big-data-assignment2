#!/usr/bin/env python3

import csv
import sys
from pathlib import Path

from cassandra.cluster import Cluster


def read_tsv_rows(path):
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.reader(handle, delimiter="\t")
        for row in reader:
            if row:
                yield row


def load_postings(session, staging_dir):
    rows_path = staging_dir / "postings.tsv"
    insert_statement = session.prepare(
        """
        INSERT INTO postings_by_term (term, doc_id, title, term_frequency)
        VALUES (?, ?, ?, ?)
        """
    )

    loaded = 0
    for row in read_tsv_rows(rows_path):
        _, term, doc_id, title, term_frequency = row
        session.execute(insert_statement, (term, doc_id, title, int(term_frequency)))
        loaded += 1

    return loaded


def load_doc_stats(session, staging_dir):
    rows_path = staging_dir / "doc_stats.tsv"
    insert_statement = session.prepare(
        """
        INSERT INTO doc_stats (doc_id, title, doc_length)
        VALUES (?, ?, ?)
        """
    )

    loaded = 0
    for row in read_tsv_rows(rows_path):
        _, doc_id, title, doc_length = row
        session.execute(insert_statement, (doc_id, title, int(doc_length)))
        loaded += 1

    return loaded


def load_corpus_stats(session, staging_dir):
    rows_path = staging_dir / "corpus_stats.tsv"
    insert_statement = session.prepare(
        """
        INSERT INTO corpus_stats (
            stats_key,
            document_count,
            total_doc_length,
            average_doc_length
        )
        VALUES (?, ?, ?, ?)
        """
    )

    loaded = 0
    for row in read_tsv_rows(rows_path):
        _, document_count, total_doc_length = row
        doc_count = int(document_count)
        total_length = int(total_doc_length)
        average_length = total_length / doc_count if doc_count else 0.0
        session.execute(
            insert_statement,
            ("global", doc_count, total_length, average_length),
        )
        loaded += 1

    return loaded


def load_vocabulary(session, staging_dir):
    rows_path = staging_dir / "vocabulary.tsv"
    insert_statement = session.prepare(
        """
        INSERT INTO vocabulary (term, document_frequency)
        VALUES (?, ?)
        """
    )

    loaded = 0
    for row in read_tsv_rows(rows_path):
        _, term, document_frequency = row
        session.execute(insert_statement, (term, int(document_frequency)))
        loaded += 1

    return loaded


def main():
    staging_dir = Path(sys.argv[1])
    cassandra_host = sys.argv[2]
    keyspace = sys.argv[3]

    cluster = Cluster([cassandra_host])
    session = cluster.connect()

    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """
    )
    session.set_keyspace(keyspace)

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS postings_by_term (
            term text,
            doc_id text,
            title text,
            term_frequency int,
            PRIMARY KEY (term, doc_id)
        )
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS doc_stats (
            doc_id text PRIMARY KEY,
            title text,
            doc_length int
        )
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS corpus_stats (
            stats_key text PRIMARY KEY,
            document_count int,
            total_doc_length int,
            average_doc_length double
        )
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS vocabulary (
            term text PRIMARY KEY,
            document_frequency int
        )
        """
    )

    session.execute("TRUNCATE postings_by_term")
    session.execute("TRUNCATE doc_stats")
    session.execute("TRUNCATE corpus_stats")
    session.execute("TRUNCATE vocabulary")

    postings_count = load_postings(session, staging_dir)
    doc_stats_count = load_doc_stats(session, staging_dir)
    corpus_stats_count = load_corpus_stats(session, staging_dir)
    vocabulary_count = load_vocabulary(session, staging_dir)

    print(f"Loaded {postings_count} posting rows into {keyspace}.postings_by_term")
    print(f"Loaded {doc_stats_count} document rows into {keyspace}.doc_stats")
    print(f"Loaded {corpus_stats_count} corpus rows into {keyspace}.corpus_stats")
    print(f"Loaded {vocabulary_count} vocabulary rows into {keyspace}.vocabulary")

    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
