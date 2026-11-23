# CI51 Big Data Lab: Docker Environment & Program Repository

A complete, self-contained Docker practice machine and code repository for the **CI51 Big Data Lab**. This environment replicates university lab systems on any Windows, macOS, or Linux laptop.

---

## Environment Specifications

| Component | Lab System | Container Environment |
|---|---|---|
| **OS / User** | Ubuntu, `rit@rit` | Ubuntu 22.04 LTS, `rit@rit` |
| **Java** | OpenJDK 17.0.20.1 | OpenJDK 17.0.20.1 |
| **Hadoop** | Hadoop 3.2.2 (Standalone & Pseudo-Distributed HDFS) | Hadoop 3.2.2 |
| **Spark** | Apache Spark 4.0.1 (Hadoop 3) | Apache Spark 4.0.1 |
| **Python** | Python 3.10 with PySpark, NumPy, Pandas, Scipy | Python 3.10 with PySpark, NumPy, Pandas, Scipy |
| **Networking** | Socket utilities (`netcat-openbsd`) | `nc` pre-installed for streaming labs |
| **Datasets** | Lab `DataSet` folder | `~/DataSet`, identical files and names |

---

## Quick Start Guide

### 1. Prerequisites
* Install [Docker Desktop](https://www.docker.com/products/docker-desktop/) and ensure the Docker engine is running.
* Windows users: Ensure "Use the WSL 2 based engine" is enabled in Docker settings.

### 2. Configure Your USN
In `.env`:
```env
USN=1MS24CI134
```

### 3. Launch the Lab
* **Windows**: Double-click `start-lab.bat` (or run it again for additional terminal sessions).
* **macOS / Linux**: Open Terminal in this folder and run:
  ```bash
  ./start-lab.sh
  ```
* **Direct Docker Command**:
  ```bash
  docker compose up -d lab
  docker compose exec lab bash
  ```

---

## Repository Structure

```text
bigdata-lab/
├── conf/                          # Hadoop and HDFS configuration templates
├── DataSet/                       # Lab datasets
│   ├── customers.txt              # Customer records
│   ├── earthquake_data.csv        # Earthquake magnitude & depth data
│   ├── employees_data.txt         # Employee salary and department data
│   ├── insurance_data.csv         # Real estate insurance policy data
│   ├── odd_even.txt               # Number dataset
│   ├── orders.txt                 # Order transaction records
│   ├── retail_transactions.txt    # Retail transactions for broadcast/accumulator
│   ├── sales_records.csv          # Global sales records
│   ├── student_details.txt        # Student database details
│   ├── weather_data.txt           # Weather report observations
│   └── wordcount_input.txt        # Sample text for WordCount MapReduce
├── scripts/                       # Startup scripts, helpers, and bashrc configuration
├── shared/                        # Host-container shared directory (mounted to /home/rit/shared)
│   └── 1MS24CI134/                # Student program directory
│       ├── spark/                 # PySpark Programs (Programs 4–12 + Practice)
│       │   ├── Bash.sh
│       │   ├── pair_rdd.py        # Program 4: Pair RDD operations
│       │   ├── weather.py         # Program 5: Weather analysis
│       │   ├── sales.py           # Program 6: Sales records analysis
│       │   ├── retail.py          # Program 7: Broadcast & Accumulators
│       │   ├── employee_crud.py   # Program 8: Spark SQL & CRUD
│       │   ├── sports_analytics.py# Program 9: Real-time sports streaming
│       │   ├── log_analysis.py    # Program 10: Real-time log streaming
│       │   ├── customer_analytics.py # Program 11: MLlib Feature Engineering
│       │   ├── fraud_train.py     # Program 12 (Part 1): Train Logistic Regression model
│       │   ├── fraud_stream.py    # Program 12 (Part 2): Real-time streaming fraud detection
│       │   ├── fraud_model/       # Saved trained model
│       │   ├── movie_recommendation.py # Program 13: Collaborative Filtering with ALS
│       │   └── practice.py        # Practice template scratchpad
│       └── hadoop/                # Hadoop MapReduce Programs (Programs 1–3)
│           ├── wordcount/         # Program 2: WordCount MapReduce
│           ├── employee/          # Program 3a: Employee Salary MapReduce
│           └── insurance/         # Program 3b: Insurance Analysis MapReduce
├── docker-compose.yml             # Docker services definition
├── Dockerfile                     # Image definition
├── start-lab.bat                  # One-click Windows launch script
└── start-lab.sh                   # Unix launch script
```

---

## Lab Programs Reference & Execution

Before running commands inside the container, navigate to your target folder and source the environment variables:

```bash
cd ~/1MS24CI134/spark
source Bash.sh
```

---

### Program 1: HDFS Operations (Pseudo-Distributed Mode)
1. Turn on HDFS daemons:
   ```bash
   hdfs-on
   ```
2. Run standard HDFS file management commands:
   ```bash
   hdfs dfs -mkdir /user
   hdfs dfs -mkdir /user/rit
   hdfs dfs -put ~/DataSet/wordcount_input.txt /user/rit/input.txt
   hdfs dfs -ls /user/rit
   hdfs dfs -cat /user/rit/input.txt
   ```
3. Web UI: Visit [http://localhost:9870](http://localhost:9870) in your browser.
4. When finished with HDFS commands:
   ```bash
   hdfs-off
   ```

---

### Program 2: WordCount (MapReduce)
```bash
cd ~/1MS24CI134/hadoop-3.2.2/Programs/wordcount
javac -classpath $(hadoop classpath) -d . mapper.java reducer.java driver.java
jar cfm wordcount.jar manifest.txt wordcount/*.class
hadoop jar wordcount.jar wordcount.driver input.txt output
cat output/part-00000
```

---

### Program 3: Employee & Insurance (MapReduce)
```bash
cd ~/1MS24CI134/hadoop-3.2.2/Programs/employee
javac -classpath $(hadoop classpath) -d . mapper.java reducer.java driver.java
jar cfm employee.jar manifest.txt employee/*.class
hadoop jar employee.jar employee.driver input.txt output
cat output/part-00000
```

---

### Program 4: Pair RDD Transformations & Actions
```bash
cd ~/1MS24CI134/spark
source Bash.sh
spark-submit pair_rdd.py
```
*Demonstrates `map()`, `filter()`, `mapValues()`, `reduceByKey()`, `groupByKey()`, `combineByKey()`, `collect()`, `count()`, `first()`, `take()`, `countByKey()`, `lookup()`, and `reduce()`.*

---

### Program 5: Weather Data Analysis
```bash
cd ~/1MS24CI134/spark
source Bash.sh
spark-submit weather.py weather_data.txt output_min output_max
cat output_min/part-00000
cat output_max/part-00000
```

---

### Program 6: Sales Records Analysis
```bash
cd ~/1MS24CI134/spark
source Bash.sh
spark-submit sales.py sales.csv out_units out_total out_channel
```

---

### Program 7: Retail Transactions (Broadcast & Accumulators)
```bash
cd ~/1MS24CI134/spark
source Bash.sh
spark-submit retail.py retail_input.txt
```
*Validates transactions with broadcast dictionary and counts invalid records using an accumulator.*

---

### Program 8: Employee CRUD & SQL Aggregations
```bash
cd ~/1MS24CI134/spark
source Bash.sh
spark-submit employee_crud.py
```
*Demonstrates DataFrame creation, temp views, SQL queries for insert, update, delete, and aggregate statistics (`COUNT`, `SUM`, `AVG`, `MAX`, `MIN`).*

---

### Program 9: Real-Time Sports Analytics (Structured Streaming)
Requires **2 terminals**:

* **Terminal 1 (Socket Stream Producer)**:
  ```bash
  nc -lk 9999
  ```
* **Terminal 2 (Spark Application)**:
  ```bash
  cd ~/1MS24CI134/spark
  source Bash.sh
  spark-submit sports_analytics.py
  ```
* In Terminal 1, type or paste commentary lines (e.g. `Kohli hits a four`, `Rohit scores six runs`). Terminal 2 updates counts every 5 seconds.

---

### Program 10: Real-Time Log Analysis (Structured Streaming)
Requires **2 terminals**:

* **Terminal 1 (Socket Stream Producer)**:
  ```bash
  nc -lk 9999
  ```
* **Terminal 2 (Spark Application)**:
  ```bash
  cd ~/1MS24CI134/spark
  source Bash.sh
  spark-submit log_analysis.py
  ```
* Send log entries with levels `INFO`, `ERROR`, `WARNING`, `DEBUG` to see live counts.

---

### Program 11: Customer Analytics (MLlib Feature Engineering)
```bash
cd ~/1MS24CI134/spark
source Bash.sh
spark-submit customer_analytics.py
```
*Performs feature extraction and pipeline transformation using `StringIndexer`, `Tokenizer`, `HashingTF`, and `VectorAssembler` on `customers.csv`.*

---

### Program 12: Real-Time Fraud Detection (MLlib Logistic Regression + Streaming)
1. **Train and Save the Model**:
   ```bash
   cd ~/1MS24CI134/spark
   source Bash.sh
   spark-submit fraud_train.py
   ```
2. **Terminal 1 (Socket Producer)**:
   ```bash
   nc -lk 9999
   ```
3. **Terminal 2 (Streaming Detection)**:
   ```bash
   cd ~/1MS24CI134/spark
   source Bash.sh
   spark-submit fraud_stream.py
   ```
4. Paste transaction streams into Terminal 1 (`TX101,1200.0,0`, `TX102,65000.0,1`). Terminal 2 outputs real-time predictions (`0.0` = Normal, `1.0` = Fraud).

---

### Program 13: Movie Recommendation System (MLlib ALS)
```bash
cd ~/1MS24CI134/spark
source Bash.sh
spark-submit movie_recommendation.py
```
*Builds a collaborative filtering recommendation model using Alternating Least Squares (ALS), evaluates predictions with Root Mean Squared Error (RMSE), and outputs top 3 movie recommendations for every user.*

---

## Practice Scratchpad

To experiment with new code:
1. Open and edit [shared/1MS24CI134/spark/practice.py](shared/1MS24CI134/spark/practice.py).
2. Run it inside the container:
   ```bash
   cd ~/1MS24CI134/spark
   source Bash.sh
   spark-submit practice.py
   ```

---

## Helpful Commands

* **`lab-help`**: Shows folder locations and shortcut commands inside the container.
* **`lab-reset`**: Resets the container to exam-start state (backs up existing work to `~/lab-backup-<time>/`).
* **`docker compose stop`**: Shuts down the container safely from host (preserves all data in Docker volumes and `shared/`).
