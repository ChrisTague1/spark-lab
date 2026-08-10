# Apache Spark Local Lab

A minimal Apache Spark lab for running Spark locally on macOS using Python.

The goal is just to:

1. Install PySpark
2. Start Spark locally
3. Play with DataFrames
4. Run some SQL
5. Run a standalone Spark job
6. Take it from there

---

## 1. Create the Project

```bash
mkdir spark-lab
cd spark-lab
```

```bash
mise use java@temurin-17
```

> Configures the java version

```bash
uv init
uv venv
```

```bash
uv add pyspark jupyterlab
```

Verify it:

```bash
uv run pyspark --version
```

---

## 2. Start JupyterLab

Launch JupyterLab from the project directory:

```bash
uv run jupyter lab
```

Your browser should open automatically. In JupyterLab, create a notebook by selecting
**Python 3 (ipykernel)** under **Notebook**.

Unlike the `pyspark` terminal shell, a regular Jupyter notebook does not create a
Spark session automatically. Put this in the first cell and run it:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SparkLabNotebook")
    .master("local[*]")
    .getOrCreate()
)
```

Check that Spark is ready in another cell:

```python
spark.version
```

Keep this notebook open for the exercises below. Each Python block can be pasted
into its own cell and run with **Shift+Enter**. Variables such as `spark` and `df`
remain available between cells.

When you are finished, stop Spark in a final cell:

```python
spark.stop()
```

Then use **File > Shut Down** in JupyterLab and press **Ctrl+C** in the terminal
that launched it.

---

# 3. Create Your First DataFrame

Inside your Jupyter notebook:

```python
data = [
    ("Alice", 25, "Chicago"),
    ("Bob", 32, "New York"),
    ("Charlie", 28, "Chicago"),
    ("David", 41, "Boston"),
    ("Eve", 35, "Chicago"),
]
```

Create a DataFrame:

```python
df = spark.createDataFrame(
    data,
    ["name", "age", "city"]
)
```

Display it:

```python
df.show()
```

Inspect the schema:

```python
df.printSchema()
```

---

# 4. Basic DataFrame Operations

Select columns:

```python
df.select("name", "age").show()
```

Filter rows:

```python
df.filter(df.age > 30).show()
```

Another way:

```python
df.filter("age > 30").show()
```

Filter Chicago:

```python
df.filter(df.city == "Chicago").show()
```

Sort:

```python
df.orderBy("age").show()
```

Descending:

```python
from pyspark.sql.functions import desc

df.orderBy(desc("age")).show()
```

---

# 5. Transform Some Data

Import Spark functions:

```python
from pyspark.sql import functions as F
```

Add a column:

```python
older = df.withColumn(
    "age_next_year",
    F.col("age") + 1
)
```

```python
older.show()
```

Uppercase names:

```python
df.withColumn(
    "name",
    F.upper("name")
).show()
```

Group by city:

```python
df.groupBy("city").count().show()
```

Average age by city:

```python
df.groupBy("city").agg(
    F.avg("age").alias("average_age")
).show()
```

---

# 6. Spark SQL

Spark DataFrames can be exposed as SQL tables.

Register the DataFrame:

```python
df.createOrReplaceTempView("people")
```

Now use SQL:

```python
spark.sql("""
    SELECT *
    FROM people
""").show()
```

Try something slightly more interesting:

```python
spark.sql("""
    SELECT
        city,
        COUNT(*) AS people,
        AVG(age) AS average_age
    FROM people
    GROUP BY city
    ORDER BY people DESC
""").show()
```

---

# 7. Look at Spark's Query Plan

One important thing about Spark is that transformations are generally **lazy**.

For example:

```python
result = (
    df
    .filter(F.col("age") > 25)
    .groupBy("city")
    .agg(F.avg("age"))
)
```

At this point Spark hasn't necessarily performed the whole computation yet.

Look at the execution plan:

```python
result.explain()
```

Or get more detail:

```python
result.explain("formatted")
```

Now trigger an **action**:

```python
result.show()
```

Other common actions include:

```python
df.count()
```

```python
df.collect()
```

```python
df.first()
```

Spark's distinction between **transformations** and **actions** is worth experimenting with.

---

# 8. Create Some Actual Data

Create a CSV:

```bash
cat > people.csv <<'EOF'
name,age,city
Alice,25,Chicago
Bob,32,New York
Charlie,28,Chicago
David,41,Boston
Eve,35,Chicago
Frank,29,Boston
Grace,38,New York
Henry,22,Chicago
EOF
```

Read the file:

```python
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("people.csv")
)
```

Then:

```python
df.show()
```

```python
df.printSchema()
```

```python
df.groupBy("city").count().show()
```

---

# 9. Write Data

Write the DataFrame as Parquet:

```python
df.write.mode("overwrite").parquet("people.parquet")
```

Read it back:

```python
parquet_df = spark.read.parquet("people.parquet")
```

```python
parquet_df.show()
```

Look at what Spark actually created:

```bash
ls -lah people.parquet
```

Notice that Spark wrote a **directory containing part files**, rather than one normal file.

---

# 10. Run a Spark Application

Instead of working interactively, create:

```text
job.py
```

Put this in it:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder
    .appName("SparkLab")
    .master("local[*]")
    .getOrCreate()
)


df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("people.csv")
)


result = (
    df
    .groupBy("city")
    .agg(
        F.count("*").alias("people"),
        F.avg("age").alias("average_age"),
    )
    .orderBy(F.desc("people"))
)


result.show()


spark.stop()
```

Run it using:

```bash
uv run spark-submit job.py
```

---

# 11. A Few Things to Poke At

At this point you have a working local Spark environment.

Here are some commands worth experimenting with.

### Number of partitions

```python
df.rdd.getNumPartitions()
```

### Repartition the data

```python
df2 = df.repartition(4)
```

```python
df2.rdd.getNumPartitions()
```

### See the execution plan

```python
df2.groupBy("city").count().explain("formatted")
```

### Cache something

```python
df.cache()
```

```python
df.count()
```

```python
df.is_cached
```

Remove it:

```python
df.unpersist()
```

### Generate a larger dataset

```python
big = spark.range(10_000_000)
```

```python
big.count()
```

Transform it:

```python
result = (
    big
    .withColumn("bucket", F.col("id") % 100)
    .groupBy("bucket")
    .count()
)
```

```python
result.show()
```

Look at its plan:

```python
result.explain("formatted")
```

---

# 12. Useful Commands

Start JupyterLab:

```bash
uv run jupyter lab
```

Run a Spark program:

```bash
uv run spark-submit job.py
```

Check Spark version:

```bash
uv run pyspark --version
```

Activate your Python environment:

```bash
source .venv/bin/activate
```

Stop Spark in the notebook:

```python
spark.stop()
```

---

# Where to Go From Here

You now have enough running locally to start experimenting on your own.

Some good concepts to investigate next are:

* transformations vs actions
* lazy evaluation
* partitions
* shuffles
* narrow vs wide transformations
* `repartition()` vs `coalesce()`
* Spark SQL
* joins
* Parquet
* caching/persistence
* execution plans
* executors and drivers
* Spark UI
* `spark-submit`
* cluster managers
* Spark Connect

For now, though, don't worry about Hadoop, YARN, Kubernetes, workers, executors on separate machines, or any cluster infrastructure.

Everything here is running on **one Mac**, with Spark using your local CPU cores through:

```python
.master("local[*]")
```

That's enough to learn the Spark programming model before adding distributed infrastructure.
