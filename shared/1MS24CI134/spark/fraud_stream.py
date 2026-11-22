from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegressionModel

# Create Spark Session
spark = SparkSession.builder \
    .appName("RealTimeFraudDetection") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Load pre-trained model
model = LogisticRegressionModel.load("fraud_model")

# Read live transactions from TCP socket
transactions = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Split incoming transaction data
data = transactions.select(
    split(col("value"), ",").alias("fields")
)

# Extract transaction attributes
data = data.select(
    col("fields")[0].alias("TransactionID"),
    col("fields")[1].cast("double").alias("Amount"),
    col("fields")[2].cast("double").alias("ForeignTransaction")
)

# Remove invalid transactions
data = data.dropna()

# Combine numerical features
assembler = VectorAssembler(
    inputCols=["Amount", "ForeignTransaction"],
    outputCol="features"
)

data = assembler.transform(data)

# Apply pre-trained machine learning model
predictions = model.transform(data)

# Identify transaction status
results = predictions.select(
    "TransactionID",
    "Amount",
    "ForeignTransaction",
    "prediction"
)

# Display real-time predictions
query = results.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
