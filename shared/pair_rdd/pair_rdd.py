from pyspark import SparkContext

sc = SparkContext("local[*]", "PairRDDOperations")
sc.setLogLevel("ERROR")

data = [
    ("A", 10),
    ("B", 20),
    ("A", 30),
    ("C", 40),
    ("B", 50),
    ("A", 60)
]

pair_rdd = sc.parallelize(data)

mapped_rdd = pair_rdd.map(lambda x: (x[0], x[1] * 2))
filtered_rdd = pair_rdd.filter(lambda x: x[1] > 25)
mapped_values = pair_rdd.mapValues(lambda x: x * 2)
sum_by_key = pair_rdd.reduceByKey(lambda x, y: x + y)
grouped_rdd = pair_rdd.groupByKey()

create_combiner = lambda value: (value, 1)
merge_value = lambda acc, value: (acc[0] + value, acc[1] + 1)
merge_combiners = lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])

print("\nOriginal Pair RDD:")
print(pair_rdd.collect())

print("\n1. map() - Multiply values by 2:")
print(mapped_rdd.collect())

print("\n2. filter() - Values greater than 25:")
print(filtered_rdd.collect())

print("\n3. mapValues() - Multiply values by 2:")
print(mapped_values.collect())

print("\n4. reduceByKey() - Sum values by key:")
print(sum_by_key.collect())

print("\n5. groupByKey() - Group values by key:")
print(grouped_rdd.mapValues(list).collect())

combined_rdd = pair_rdd.combineByKey(
    create_combiner,
    merge_value,
    merge_combiners
)

average_by_key = combined_rdd.mapValues(
    lambda total_count: total_count[0] / total_count[1]
)

print("\n6. combineByKey() - Average by key:")
print(average_by_key.collect())

print("\nAction 1 - collect():")
print(pair_rdd.collect())

print("\nAction 2 - count():")
print(pair_rdd.count())

print("\nAction 3 - first():")
print(pair_rdd.first())

print("\nAction 4 - take(3):")
print(pair_rdd.take(3))

print("\nAction 5 - countByKey():")
print(dict(pair_rdd.countByKey()))

print("\nAction 6 - lookup('A'):")
print(pair_rdd.lookup("A"))

total = pair_rdd.map(lambda x: x[1]).reduce(lambda x, y: x + y)
print("\nAction 7 - reduce():")
print(total)

sc.stop()
