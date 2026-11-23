
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col,explode,split,lower)

spark=SparkSession.builder.appName("LiveStreaming").getOrCreate()

commentary=spark.readStream.format("socket").option("host","localhost").option("port","9999").load()

keyword=["kohli","rohit","run","wicket"]

words=commentary.select(explode(split(lower(col("value")),r"\W+")).alias("word"))
filtered_words=words.filter(col("word").isin(keyword))
keyword_count=filtered_words.groupBy("word").count()

query=keyword_count.writeStream.outputMode("complete").format("console").option("truncate",False).trigger(processingTime="5 seconds").start()
query.awaitTermination()