from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, Tokenizer, HashingTF, VectorAssembler

spark = SparkSession.builder.appName("CustomerAnalytics").getOrCreate()

df = spark.read.csv("customers.csv", header=True, inferSchema=True)

print("Original Customer Data")
df.show()

print("Dataset Schema")
df.printSchema()
print("Number of Records:", df.count())

df = df.dropna()

gender = StringIndexer(inputCol="Gender", outputCol="GenderIndex")
city = StringIndexer(inputCol="City", outputCol="CityIndex")

df = gender.fit(df).transform(df)
df = city.fit(df).transform(df)

tokenizer = Tokenizer(inputCol="Review", outputCol="Words")
df = tokenizer.transform(df)

tf = HashingTF(inputCol="Words", outputCol="TextFeatures", numFeatures=10)
df = tf.transform(df)

assembler = VectorAssembler(
    inputCols=["GenderIndex", "CityIndex", "TextFeatures"],
    outputCol="Features"
)
df = assembler.transform(df)

print("Processed Customer Data")
df.select("CustomerID", "GenderIndex", "CityIndex", "Words", "TextFeatures", "Features").show(truncate=False)

spark.stop()
