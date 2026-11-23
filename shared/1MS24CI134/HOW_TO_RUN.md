# CI51 Big Data Lab: Complete Execution Guide (Programs 1 to 13)

This guide provides the exact commands, inputs, and expected outputs to run every program in the lab.

---

## Quick Reference Table

| Prog # | Title | Technology | Primary File | Key Input |
|:---:|---|---|---|---|
| **1** | HDFS Operations & Management | Hadoop HDFS CLI | Built-in CLI commands | `~/DataSet/wordcount_input.txt` |
| **2** | WordCount MapReduce | Hadoop Java MapReduce | `wordcount.jar` (`driver.java`, etc.) | `input.txt` |
| **3a** | Employee Salary Processing | Hadoop Java MapReduce | `employee.jar` | `input.txt` |
| **3b** | Insurance Data Processing | Hadoop Java MapReduce | `insurance.jar` | `insurance.csv` |
| **4** | Pair RDD Transformations & Actions | PySpark RDD | `pair_rdd.py` | Embedded data |
| **5** | Weather Data Analysis (Min/Max Temp) | PySpark RDD | `weather.py` | `weather_data.txt` |
| **6** | Sales Records Analysis | PySpark RDD | `sales.py` | `sales.csv` |
| **7** | Retail Transactions (Broadcast & Accumulator) | PySpark RDD | `retail.py` | `retail_input.txt` |
| **8** | Employee CRUD & SQL Aggregations | Spark SQL | `employee_crud.py` | Embedded DataFrame |
| **9** | Real-Time Sports Analytics | Structured Streaming | `sports_analytics.py` | Socket `nc -lk 9999` |
| **10** | Real-Time Log Analysis | Structured Streaming | `log_analysis.py` | Socket `nc -lk 9999` |
| **11** | Customer Analytics (MLlib Feature Pipeline) | Spark MLlib | `customer_analytics.py` | `customers.csv` |
| **12** | Real-Time Fraud Detection | MLlib + Streaming | `fraud_train.py`, `fraud_stream.py` | `fraud_model`, Socket `9999` |
| **13** | Movie Recommendation System (ALS) | Spark MLlib | `movie_recommendation.py` | Embedded ratings |

---

## How to Access the Lab Environment

### Method 1: Interactive Container Terminal (Recommended)
* **Windows**: Double-click `start-lab.bat` or run:
  ```powershell
  docker compose exec lab bash
  ```
* **macOS / Linux**:
  ```bash
  ./start-lab.sh
  ```

### Method 2: Direct Execution from Windows Host PowerShell
You can run any command directly from Windows PowerShell without entering the shell:
```powershell
docker compose exec lab bash -c "<commands>"
```

---

## Detailed Step-by-Step Instructions

---

### Program 1: HDFS Operations & Management

```bash
# 1. Start single-node HDFS daemons
hdfs-on

# 2. Load Hadoop environment
cd ~/1MS24CI134/hadoop-3.2.2
source Bash.sh

# 3. Create directory in HDFS
hdfs dfs -mkdir -p /user/rit

# 4. Upload file from local filesystem to HDFS
hdfs dfs -put ~/DataSet/wordcount_input.txt /user/rit/input.txt

# 5. List files in HDFS directory
hdfs dfs -ls /user/rit

# 6. Read file contents directly from HDFS
hdfs dfs -cat /user/rit/input.txt

# 7. Copy file from HDFS back to local system
hdfs dfs -get /user/rit/input.txt ~/downloaded_sample.txt

# 8. Web UI Check (Open in browser on host)
# http://localhost:9870

# 9. Turn off HDFS when finished
hdfs-off
```

---

### Program 2: WordCount (Hadoop MapReduce)

```bash
cd ~/1MS24CI134/hadoop-3.2.2/Programs/wordcount
source ~/1MS24CI134/hadoop-3.2.2/Bash.sh

# 1. Clean previous outputs (if any)
rm -rf output wordcount/*.class wordcount.jar

# 2. Compile Java source files
javac -classpath $(hadoop classpath) -d . mapper.java reducer.java driver.java

# 3. Package into executable JAR
jar cfm wordcount.jar manifest.txt wordcount/*.class

# 4. Run MapReduce Job
hadoop jar wordcount.jar wordcount.driver input.txt output

# 5. View Result
cat output/part-00000
```

---

### Program 3a: Employee Salary Analysis (Hadoop MapReduce)

```bash
cd ~/1MS24CI134/hadoop-3.2.2/Programs/employee
source ~/1MS24CI134/hadoop-3.2.2/Bash.sh

# 1. Clean previous outputs
rm -rf output employee/*.class employee.jar

# 2. Compile
javac -classpath $(hadoop classpath) -d . mapper.java reducer.java driver.java

# 3. Create JAR
jar cfm employee.jar manifest.txt employee/*.class

# 4. Run Job
hadoop jar employee.jar employee.driver input.txt output

# 5. View Result
cat output/part-00000
```

---

### Program 3b: Insurance Data Analysis (Hadoop MapReduce)

```bash
cd ~/1MS24CI134/hadoop-3.2.2/Programs/insurance
source ~/1MS24CI134/hadoop-3.2.2/Bash.sh

# 1. Clean previous outputs
rm -rf output insurance/*.class insurance.jar

# 2. Compile
javac -classpath $(hadoop classpath) -d . mapper.java reducer.java driver.java

# 3. Create JAR
jar cfm insurance.jar manifest.txt insurance/*.class

# 4. Run Job
hadoop jar insurance.jar insurance.driver insurance.csv output

# 5. View Result
cat output/part-00000
```

---

### Program 4: Pair RDD Transformations & Actions (PySpark)

```bash
cd ~/1MS24CI134/spark
source Bash.sh

spark-submit pair_rdd.py
```

* **What it demonstrates**: `map()`, `filter()`, `mapValues()`, `reduceByKey()`, `groupByKey()`, `combineByKey()`, `collect()`, `count()`, `first()`, `take()`, `countByKey()`, `lookup()`, `reduce()`.

---

### Program 5: Weather Data Analysis (PySpark)

```bash
cd ~/1MS24CI134/spark
source Bash.sh

# Clean old output folders
rm -rf output_min output_max

# Run Job
spark-submit weather.py weather_data.txt output_min output_max

# View Results
echo "--- Minimum Temperatures by Year ---"
cat output_min/part-00000

echo "--- Maximum Temperatures by Year ---"
cat output_max/part-00000
```

---

### Program 6: Sales Records Analysis (PySpark)

```bash
cd ~/1MS24CI134/spark
source Bash.sh

# Clean old outputs
rm -rf out_units out_total out_channel

# Run Job
spark-submit sales.py sales.csv out_units out_total out_channel

# View Results
cat out_units/part-00000
cat out_total/part-00000
cat out_channel/part-00000
```

---

### Program 7: Retail Transactions (Broadcast & Accumulator)

```bash
cd ~/1MS24CI134/spark
source Bash.sh

spark-submit retail.py retail_input.txt
```

* **Expected Output**:
  ```text
  Valid Transactions:
  ('T001', 'P01', 'Laptop', 2)
  ('T002', 'P02', 'Mouse', 5)
  ('T005', 'P04', 'Monitor', 3)
  ('T008', 'P03', 'Keyboard', 4)

  Number of invalid transaction records = 4
  ```

---

### Program 8: Employee Database CRUD & SQL Aggregations

```bash
cd ~/1MS24CI134/spark
source Bash.sh

spark-submit employee_crud.py
```

* **Operations Demonstrated**:
  * Original table query (`SELECT *`)
  * Insert new record using `UNION ALL` (`Kiran`)
  * Update salary using `CASE WHEN` (`Rahul` to 65000)
  * Delete record (`WHERE ID != 103`)
  * Aggregations: `COUNT`, `SUM`, `AVG`, `MAX`, `MIN`

---

### Program 9: Real-Time Sports Analytics (Structured Streaming)

> [!IMPORTANT]
> Requires **2 terminals** open simultaneously.

#### Terminal 1 (Socket Stream Producer):
```bash
nc -lk 9999
```

#### Terminal 2 (Spark Streaming Application):
```bash
cd ~/1MS24CI134/spark
source Bash.sh

spark-submit sports_analytics.py
```

#### Feed Data (Paste into Terminal 1):
```text
Kohli hits a four
Rohit scores six runs
Kohli hits another six
Dhoni takes a catch
Rohit scores a boundary
Kohli completes century
Dhoni takes a wicket
Rohit is out
```

*Terminal 2 updates counts every 5 seconds. Press `Ctrl + C` in both terminals when finished.*

---

### Program 10: Real-Time Log Analysis (Structured Streaming)

> [!IMPORTANT]
> Requires **2 terminals** open simultaneously.

#### Terminal 1 (Socket Stream Producer):
```bash
nc -lk 9999
```

#### Terminal 2 (Spark Streaming Application):
```bash
cd ~/1MS24CI134/spark
source Bash.sh

spark-submit log_analysis.py
```

#### Feed Data (Paste into Terminal 1):
```text
2026-11-23 10:00:01 INFO Server started successfully
2026-11-23 10:00:05 DEBUG Initializing database connection pool
2026-11-23 10:00:12 WARNING High memory usage detected: 85%
2026-11-23 10:00:15 ERROR Database connection failed: timeout
2026-11-23 10:00:20 INFO Retrying connection to database
2026-11-23 10:00:25 ERROR Connection failed again
2026-11-23 10:00:30 INFO Service restarted
```

---

### Program 11: Customer Analytics (MLlib Feature Pipeline)

```bash
cd ~/1MS24CI134/spark
source Bash.sh

spark-submit customer_analytics.py
```

* **Pipeline Stages**:
  1. `StringIndexer`: Gender (`GenderIndex`) & City (`CityIndex`)
  2. `Tokenizer`: Review text into individual words
  3. `HashingTF`: Term frequency vector (`numFeatures=10`)
  4. `VectorAssembler`: Combined feature vector (`Features`)

---

### Program 12: Real-Time Fraud Detection (MLlib + Streaming)

#### Step 1: Train and Save the Logistic Regression Model
```bash
cd ~/1MS24CI134/spark
source Bash.sh

spark-submit fraud_train.py
```
*(Saves trained model to `fraud_model` folder)*

#### Step 2: Start the Socket Stream Producer (Terminal 1)
```bash
nc -lk 9999
```

#### Step 3: Start the Streaming Prediction App (Terminal 2)
```bash
cd ~/1MS24CI134/spark
source Bash.sh

spark-submit fraud_stream.py
```

#### Step 4: Feed Live Transactions (Paste into Terminal 1)
```text
TX101,1200.0,0
TX102,65000.0,1
TX103,1800.0,1
TX104,80000.0,1
TX105,2500.0,0
TX106,70000.0,0
```
*Terminal 2 will output real-time predictions (`0.0` = Legitimate, `1.0` = Fraud).*

---

### Program 13: Movie Recommendation System (MLlib ALS)

```bash
cd ~/1MS24CI134/spark
source Bash.sh

spark-submit movie_recommendation.py
```

* **Output Sections**:
  * Original User-Movie Rating dataset
  * Predicted ratings on test split
  * Root Mean Squared Error (RMSE) evaluation
  * Top 3 personalized movie recommendations for every user

---

## Free Practice Scratchpad (`practice.py`)

To practice writing custom PySpark code without altering original lab solutions:

1. Edit [practice.py](shared/1MS24CI134/spark/practice.py) in VS Code.
2. In the container terminal, run:
   ```bash
   cd ~/1MS24CI134/spark
   source Bash.sh
   spark-submit practice.py [optional_input_file]
   ```
*(All changes saved in VS Code are instantly live inside the container).*
