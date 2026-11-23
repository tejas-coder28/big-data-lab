from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("EmployeeCRUD").getOrCreate()

data = [
    (101, "Rahul", "IT", 50000),
    (102, "Priya", "HR", 45000),
    (103, "Amit", "Finance", 60000),
    (104, "Sneha", "IT", 55000)
]

columns = ["ID", "Name", "Department", "Salary"]

df = spark.createDataFrame(data, columns)
df.createOrReplaceTempView("Employee")

# Read
print("--- Original Records ---")
spark.sql("SELECT * FROM Employee").show()

# Insert
print("--- After Insert (Kiran) ---")
df1 = spark.sql("""
SELECT * FROM Employee
UNION ALL
SELECT 105, 'Kiran', 'HR', 48000
""")
df1.show()
df1.createOrReplaceTempView("Employee")

# Update
print("--- After Update (Rahul Salary to 65000) ---")
df2 = spark.sql("""
SELECT ID, Name, Department,
CASE WHEN ID = 101 THEN 65000 ELSE Salary END AS Salary
FROM Employee
""")
df2.show()
df2.createOrReplaceTempView("Employee")

# Delete
print("--- After Delete (Amit ID 103) ---")
df3 = spark.sql("SELECT * FROM Employee WHERE ID != 103")
df3.show()
df3.createOrReplaceTempView("Employee")

# Aggregate
print("--- Aggregations ---")
spark.sql("SELECT COUNT(*) AS Total_Employees FROM Employee").show()
spark.sql("SELECT SUM(Salary) AS Total_Salary FROM Employee").show()
spark.sql("SELECT AVG(Salary) AS Average_Salary FROM Employee").show()
spark.sql("SELECT MAX(Salary) AS Max_Salary FROM Employee").show()
spark.sql("SELECT MIN(Salary) AS Min_Salary FROM Employee").show()

spark.stop()
