import sys
from pyspark import SparkContext

if len(sys.argv) != 5:
    print("Provide Input File and 3 Output Directories")
    sys.exit(0)

sc = SparkContext()
rows = sc.textFile(sys.argv[1]).map(lambda x: x.split(","))

rows.map(lambda c: (c[7], int(c[2]))).reduceByKey(lambda a, b: a+b).saveAsTextFile(sys.argv[2])

rows.map(lambda c: (c[7], 1)).reduceByKey(lambda a, b: a+b).saveAsTextFile(sys.argv[3])

rows.map(lambda c: (c[3], 1)).reduceByKey(lambda a, b: a+b).saveAsTextFile(sys.argv[4])