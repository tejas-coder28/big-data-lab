"""
CI51 Big Data Lab - Practice Scratchpad
Run with: spark-submit practice.py
"""

from pyspark.sql import SparkSession
from pyspark import SparkContext

# Initialize Spark Session & Context
spark = SparkSession.builder \
    .appName("PracticeSession") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

print("========================================")
print(" Spark is ready! Write your code below. ")
print("========================================")

# --- Write your practice code below ---

# Example 1: RDD
# rdd = sc.parallelize([1, 2, 3, 4, 5])
# print("Sum:", rdd.reduce(lambda x, y: x + y))

# Example 2: DataFrame
# df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["ID", "Name"])
# df.show()


# Stop Spark when done
spark.stop()
