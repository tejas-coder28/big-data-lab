from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode, split, lower, col, trim
)

spark = SparkSession.builder \
    .appName("RealTimeSportsAnalytics") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

commentary = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

words = commentary.select(
    explode(
        split(lower(col("value")), r"\W+")
    ).alias("word")
)

keywords = [
    "kohli",
    "rohit",
    "dhoni",
    "sachin",
    "four",
    "six",
    "wicket",
    "run",
    "runs",
    "boundary",
    "out",
    "catch",
    "century"
]

filtered_words = words.filter(col("word").isin(keywords))

keyword_counts = filtered_words.groupBy("word").count()

query = keyword_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
