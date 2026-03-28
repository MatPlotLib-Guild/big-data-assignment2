from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand
import unicodedata


spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()


df = spark.read.parquet("/a.parquet")
n = 100
df = df.select(['id', 'title', 'text']).orderBy(rand(seed=0)).limit(n)


def create_doc(row):
    ascii_name = unicodedata.normalize("NFKD", str(row["id"]) + "_" + row["title"])
    ascii_name = ascii_name.encode("ascii", "ignore").decode("ascii")
    filename = "data/" + sanitize_filename(ascii_name).replace(" ", "_") + ".txt"
    with open(filename, "w") as f:
        f.write(row['text'])


df.foreach(create_doc)


# df.write.csv("/index/data", sep = "\t")