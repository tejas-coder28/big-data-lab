from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

spark = SparkSession.builder.appName("LogAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

logs = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

logs = logs.withColumn("level", regexp_extract("value", r"\b(INFO|ERROR|WARNING|DEBUG)\b", 1))

result = logs.filter("level != ''").groupBy("level").count()

query = result.writeStream.format("console").outputMode("complete").start()
query.awaitTermination()
