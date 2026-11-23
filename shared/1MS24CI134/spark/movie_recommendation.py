from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

# Create Spark Session
spark = SparkSession.builder \
    .appName("MovieRecommendation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Sample movie ratings
data = [
    (1, 101, 5.0),
    (1, 102, 3.0),
    (1, 103, 4.0),
    (2, 101, 4.0),
    (2, 102, 2.0),
    (2, 104, 5.0),
    (3, 101, 2.0),
    (3, 103, 5.0),
    (3, 104, 4.0),
    (4, 102, 4.0),
    (4, 103, 3.0),
    (4, 104, 5.0),
    (5, 101, 5.0),
    (5, 102, 4.0),
    (5, 104, 3.0)
]

columns = ["UserID", "MovieID", "Rating"]

df = spark.createDataFrame(data, columns)

print("Movie Ratings Dataset")
df.show()

# Split data into training and testing
train, test = df.randomSplit([0.8, 0.2], seed=42)

# Create ALS model
als = ALS(
    userCol="UserID",
    itemCol="MovieID",
    ratingCol="Rating",
    rank=2,
    maxIter=10,
    regParam=0.1,
    coldStartStrategy="drop"
)

# Train the model
model = als.fit(train)

# Predict ratings on test data
predictions = model.transform(test)

print("Predicted Movie Ratings")
predictions.show()

# Evaluate the model
evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="Rating",
    predictionCol="prediction"
)

if predictions.count() > 0:
    rmse = evaluator.evaluate(predictions)
    print("Root Mean Squared Error (RMSE):", rmse)
else:
    print("No test predictions available for evaluation.")

# Generate top 3 movie recommendations for each user
recommendations = model.recommendForAllUsers(3)

print("Top 3 Movie Recommendations")
recommendations.show(truncate=False)

spark.stop()
