import sys
if(len(sys.argv)!=4):
    print("Provide Input File and Output Directory")
    sys.exit(0)

from pyspark import SparkContext
sc = SparkContext()
f = sc.textFile(sys.argv[1])

temp = f.map(lambda x: (int(x[15:19]), int(x[87:92])))
minimum=temp.reduceByKey(min).saveAsTextFile(sys.argv[2])
maximum=temp.reduceByKey(max).saveAsTextFile(sys.argv[3])
