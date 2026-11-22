from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler

spark = SparkSession.builder.appName("FraudModelTraining").getOrCreate()

# Sample historical banking transactions
data = [
    (1000.0, 0, 0),
    (2000.0, 0, 0),
    (3000.0, 0, 0),
    (50000.0, 1, 1),
    (75000.0, 1, 1),
    (90000.0, 1, 1),
    (1500.0, 1, 0),
    (65000.0, 0, 1)
]

columns = ["Amount", "ForeignTransaction", "Fraud"]

df = spark.createDataFrame(data, columns)

# Combine input features
assembler = VectorAssembler(
    inputCols=["Amount", "ForeignTransaction"],
    outputCol="features"
)

df = assembler.transform(df)

# Train logistic regression model
lr = LogisticRegression(
    featuresCol="features",
    labelCol="Fraud",
    maxIter=20
)

model = lr.fit(df)

# Save trained model
model.write().overwrite().save("fraud_model")

print("Fraud Detection Model Trained and Saved Successfully")

spark.stop()
