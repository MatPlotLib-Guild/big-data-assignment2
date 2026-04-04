import sys
from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand
import unicodedata


spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()

mode = sys.argv[1] if len(sys.argv) > 1 else "docs"

def create_doc(row):
    ascii_name = unicodedata.normalize("NFKD", str(row["id"]) + "_" + row["title"])
    ascii_name = ascii_name.encode("ascii", "ignore").decode("ascii")
    filename = "data/" + sanitize_filename(ascii_name).replace(" ", "_") + ".txt"
    with open(filename, "w") as f:
        f.write(row['text'])


if mode == "docs":
    df = spark.read.parquet("/a.parquet")
    n = 100
    df = df.select(['id', 'title', 'text']).orderBy(rand(seed=0)).limit(n)
    df.foreach(create_doc)
else:
    spark.sparkContext.wholeTextFiles("/data/*").map(
        lambda path_and_text: (
            path_and_text[0].rsplit("/", 1)[-1].rsplit(".", 1)[0].split("_", 1)[0]
            + "\t"
            + path_and_text[0].rsplit("/", 1)[-1].rsplit(".", 1)[0].split("_", 1)[1].replace("_", " ")
            + "\t"
            + " ".join(path_and_text[1].split())
        )
    ).coalesce(1).saveAsTextFile("/input/data")


# df.write.csv("/index/data", sep = "\t")