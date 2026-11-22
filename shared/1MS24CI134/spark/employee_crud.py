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

print("Original Employee Records")
spark.sql("SELECT * FROM Employee").show()

print("After Inserting Employee")
df1 = spark.sql("""
SELECT * FROM Employee
UNION ALL
SELECT 105, 'Kiran', 'HR', 48000
""")
df1.show()
df1.createOrReplaceTempView("Employee1")

print("After Updating Salary")
df2 = spark.sql("""
SELECT ID, Name, Department,
CASE WHEN ID = 101 THEN 65000
ELSE Salary END AS Salary
FROM Employee1
""")
df2.show()
df2.createOrReplaceTempView("Employee2")

print("After Deleting Employee")
df3 = spark.sql("""
SELECT * FROM Employee2
WHERE ID != 103
""")
df3.show()
df3.createOrReplaceTempView("Employee3")

print("Total Number of Employees")
spark.sql("""
SELECT COUNT(*) AS Total_Employees
FROM Employee3
""").show()

print("Total Salary of Employees")
spark.sql("""
SELECT SUM(Salary) AS Total_Salary
FROM Employee3
""").show()

print("Average Salary of Employees")
spark.sql("""
SELECT AVG(Salary) AS Average_Salary
FROM Employee3
""").show()

print("Maximum Salary")
spark.sql("""
SELECT MAX(Salary) AS Maximum_Salary
FROM Employee3
""").show()

print("Minimum Salary")
spark.sql("""
SELECT MIN(Salary) AS Minimum_Salary
FROM Employee3
""").show()

spark.stop()
