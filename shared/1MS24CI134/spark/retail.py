import sys
from pyspark.sql import SparkSession

if len(sys.argv) < 2:
    print("Usage: spark-submit retail.py <input_path>")
    sys.exit(1)

spark = SparkSession.builder.appName("RetailTransactionProcessing").getOrCreate()
sc = spark.sparkContext

products = {
    "P01": "Laptop",
    "P02": "Mouse",
    "P03": "Keyboard",
    "P04": "Monitor"
}

bc_products = sc.broadcast(products)
invalid = sc.accumulator(0)

lines = sc.textFile(sys.argv[1])

def process(line):
    global invalid
    f = line.strip().split(",")

    if len(f) != 3 or f[0] == "" or f[1] not in bc_products.value or int(f[2]) <= 0:
        invalid.add(1)
        return None

    return (f[0], f[1], bc_products.value[f[1]], int(f[2]))

results = lines.map(process).filter(lambda x: x is not None).collect()

print("Valid Transactions:")
for x in results:
    print(x)

print("Number of invalid transaction records =", invalid.value)

spark.stop()
