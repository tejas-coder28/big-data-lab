from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

spark = SparkSession.builder.appName("LogAnalysis").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

logs = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

levels = logs.select(regexp_extract("value", r"(INFO|ERROR|WARNING|DEBUG)", 1).alias("level"))

counts = levels.filter("level != ''").groupBy("level").count()

query = counts.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()