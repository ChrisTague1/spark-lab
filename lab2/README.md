# Apache Spark + Hadoop/YARN Administration Lab

This lab builds a miniature distributed data platform on your Mac.

Unlike the first Spark lab, the focus here is **cluster administration**, not learning the DataFrame API.

By the end you will have:

* a 4-node logical cluster
* HDFS distributed across three workers
* YARN managing cluster resources
* Spark applications submitted through YARN
* Spark executors running on different worker nodes
* the ability to inspect jobs through YARN and Spark
* experience changing executor/resource configurations
* experience diagnosing shuffles and joins
* experience handling data skew
* experience modifying YARN queues
* experience killing workers
* experience scaling the cluster

The architecture will look roughly like:

```text
                         Your Mac
                            |
                     spark-submit
                            |
                            v
                +----------------------+
                |       master         |
                |----------------------|
                | HDFS NameNode        |
                | YARN ResourceManager |
                +----------+-----------+
                           |
             +-------------+-------------+
             |             |             |
             v             v             v

       +-----------+ +-----------+ +-----------+
       | worker-1  | | worker-2  | | worker-3  |
       |-----------| |-----------| |-----------|
       | DataNode  | | DataNode  | | DataNode  |
       |NodeManager| |NodeManager| |NodeManager|
       +-----------+ +-----------+ +-----------+
             |             |             |
             +------+------+-------------+
                    |
                 HDFS data

Spark executors will be launched inside YARN containers on the
worker nodes.
```

We are going to use:

```text
Hadoop: 3.3.6
Spark:  3.5.8
Python: 3.x
Java:   11
Docker/Colima
```

Even though your Mac already has Java, most of this lab runs inside containers, so Java will also exist inside the cluster image.

---

# 0. Before Starting

You should already have:

```bash
docker --version
docker compose version
```

If using Colima:

```bash
colima status
```

If it isn't running:

```bash
colima start --cpu 6 --memory 12
```

You do not strictly need 12 GB, but Spark + three Hadoop workers is much more interesting if the virtual machine has several CPUs and a decent amount of memory.

Check:

```bash
docker info
```

---

# 1. Create the Lab

Create:

```bash
mkdir spark-yarn-lab
cd spark-yarn-lab
```

Eventually the directory will look like:

```text
spark-yarn-lab/
├── Dockerfile
├── docker-compose.yml
├── config/
│   ├── core-site.xml
│   ├── hdfs-site.xml
│   ├── yarn-site.xml
│   ├── mapred-site.xml
│   └── capacity-scheduler.xml
└── jobs/
    ├── generate_data.py
    ├── aggregate.py
    ├── joins.py
    └── skew.py
```

Create the directories:

```bash
mkdir config jobs
```

---

# 2. Build Our Hadoop + Spark Image

Create:

```text
Dockerfile
```

with:

```dockerfile
FROM eclipse-temurin:11-jdk

ARG HADOOP_VERSION=3.3.6
ARG SPARK_VERSION=3.5.8

RUN apt-get update && \
    apt-get install -y \
        curl \
        python3 \
        python3-pip \
        procps \
        iputils-ping \
        net-tools \
        vim \
        less \
        && rm -rf /var/lib/apt/lists/*

# -------------------------------------------------------------------
# Hadoop
# -------------------------------------------------------------------

RUN curl -fsSL \
    "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
    -o /tmp/hadoop.tgz && \
    tar -xzf /tmp/hadoop.tgz -C /opt && \
    mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop && \
    rm /tmp/hadoop.tgz

# -------------------------------------------------------------------
# Spark
# -------------------------------------------------------------------

RUN curl -fsSL \
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm /tmp/spark.tgz

ENV JAVA_HOME=/opt/java/openjdk
ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark

ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV YARN_CONF_DIR=/opt/hadoop/etc/hadoop

ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin

WORKDIR /workspace

CMD ["bash"]
```

Build it:

```bash
docker build -t spark-yarn-lab .
```

This may take a little while the first time because we're downloading both Spark and Hadoop.

Verify:

```bash
docker run --rm spark-yarn-lab hadoop version
```

Then:

```bash
docker run --rm spark-yarn-lab spark-submit --version
```

---

# 3. Configure Hadoop

Now we need to configure:

```text
HDFS
YARN
MapReduce
YARN scheduling
```

---

# 4. Configure HDFS

Create:

```text
config/core-site.xml
```

```xml
<?xml version="1.0"?>
<configuration>

    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://master:9000</value>
    </property>

</configuration>
```

This tells Hadoop:

```text
When someone says:

    /some/file

they mean:

    hdfs://master:9000/some/file
```

---

Create:

```text
config/hdfs-site.xml
```

```xml
<?xml version="1.0"?>
<configuration>

    <property>
        <name>dfs.replication</name>
        <value>2</value>
    </property>

    <property>
        <name>dfs.namenode.name.dir</name>
        <value>/data/namenode</value>
    </property>

    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/data/datanode</value>
    </property>

    <property>
        <name>dfs.namenode.rpc-address</name>
        <value>master:9000</value>
    </property>

</configuration>
```

The interesting setting is:

```text
dfs.replication = 2
```

Every HDFS block should therefore normally live on **two DataNodes**.

That lets us later kill one worker without immediately losing the data.

---

# 5. Configure YARN

Create:

```text
config/yarn-site.xml
```

```xml
<?xml version="1.0"?>
<configuration>

    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>master</value>
    </property>

    <property>
        <name>yarn.resourcemanager.address</name>
        <value>master:8032</value>
    </property>

    <property>
        <name>yarn.resourcemanager.scheduler.address</name>
        <value>master:8030</value>
    </property>

    <property>
        <name>yarn.resourcemanager.resource-tracker.address</name>
        <value>master:8031</value>
    </property>

    <property>
        <name>yarn.resourcemanager.admin.address</name>
        <value>master:8033</value>
    </property>

    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>

    <!-- Resources each NodeManager advertises -->

    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>3072</value>
    </property>

    <property>
        <name>yarn.nodemanager.resource.cpu-vcores</name>
        <value>2</value>
    </property>

    <!-- Small enough allocations for our laptop cluster -->

    <property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>256</value>
    </property>

    <property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>3072</value>
    </property>

    <property>
        <name>yarn.scheduler.minimum-allocation-vcores</name>
        <value>1</value>
    </property>

    <property>
        <name>yarn.scheduler.maximum-allocation-vcores</name>
        <value>2</value>
    </property>

    <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
    </property>

</configuration>
```

Notice that every worker advertises:

```text
3072 MB memory
2 vcores
```

With three workers, our theoretical YARN cluster therefore has:

```text
Memory:

3 × 3072 MB
= 9216 MB

vcores:

3 × 2
= 6 vcores
```

This is our first important admin concept:

**YARN sees resources, not "Spark servers."**

Spark asks YARN for containers with some amount of:

```text
memory
CPU
```

YARN determines where those containers go.

---

# 6. Configure MapReduce

Create:

```text
config/mapred-site.xml
```

```xml
<?xml version="1.0"?>
<configuration>

    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>

</configuration>
```

---

# 7. Configure the YARN Scheduler

Create:

```text
config/capacity-scheduler.xml
```

```xml
<?xml version="1.0"?>
<configuration>

    <property>
        <name>yarn.scheduler.capacity.root.queues</name>
        <value>default</value>
    </property>

    <property>
        <name>yarn.scheduler.capacity.root.default.capacity</name>
        <value>100</value>
    </property>

    <property>
        <name>yarn.scheduler.capacity.root.default.maximum-capacity</name>
        <value>100</value>
    </property>

</configuration>
```

For now:

```text
root
└── default
    └── 100% cluster capacity
```

Later we'll create multiple queues.

---

# 8. Create the Cluster

Create:

```text
docker-compose.yml
```

```yaml
services:

  master:
    image: spark-yarn-lab
    hostname: master
    container_name: spark-yarn-master

    ports:
      # HDFS NameNode UI
      - "9870:9870"

      # YARN ResourceManager UI
      - "8088:8088"

    volumes:
      - ./config/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml
      - ./config/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml
      - ./config/yarn-site.xml:/opt/hadoop/etc/hadoop/yarn-site.xml
      - ./config/mapred-site.xml:/opt/hadoop/etc/hadoop/mapred-site.xml
      - ./config/capacity-scheduler.xml:/opt/hadoop/etc/hadoop/capacity-scheduler.xml
      - ./jobs:/workspace/jobs
      - namenode-data:/data/namenode

    command: >
      bash -c "
        if [ ! -d /data/namenode/current ]; then
          hdfs namenode -format -force;
        fi;
        hdfs --daemon start namenode;
        yarn --daemon start resourcemanager;
        tail -f /dev/null
      "

  worker1:
    image: spark-yarn-lab
    hostname: worker1
    container_name: spark-yarn-worker1

    volumes:
      - ./config/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml
      - ./config/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml
      - ./config/yarn-site.xml:/opt/hadoop/etc/hadoop/yarn-site.xml
      - ./config/mapred-site.xml:/opt/hadoop/etc/hadoop/mapred-site.xml
      - ./config/capacity-scheduler.xml:/opt/hadoop/etc/hadoop/capacity-scheduler.xml
      - ./jobs:/workspace/jobs
      - worker1-data:/data/datanode

    depends_on:
      - master

    command: >
      bash -c "
        hdfs --daemon start datanode;
        yarn --daemon start nodemanager;
        tail -f /dev/null
      "

  worker2:
    image: spark-yarn-lab
    hostname: worker2
    container_name: spark-yarn-worker2

    volumes:
      - ./config/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml
      - ./config/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml
      - ./config/yarn-site.xml:/opt/hadoop/etc/hadoop/yarn-site.xml
      - ./config/mapred-site.xml:/opt/hadoop/etc/hadoop/mapred-site.xml
      - ./config/capacity-scheduler.xml:/opt/hadoop/etc/hadoop/capacity-scheduler.xml
      - ./jobs:/workspace/jobs
      - worker2-data:/data/datanode

    depends_on:
      - master

    command: >
      bash -c "
        hdfs --daemon start datanode;
        yarn --daemon start nodemanager;
        tail -f /dev/null
      "

  worker3:
    image: spark-yarn-lab
    hostname: worker3
    container_name: spark-yarn-worker3

    volumes:
      - ./config/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml
      - ./config/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml
      - ./config/yarn-site.xml:/opt/hadoop/etc/hadoop/yarn-site.xml
      - ./config/mapred-site.xml:/opt/hadoop/etc/hadoop/mapred-site.xml
      - ./config/capacity-scheduler.xml:/opt/hadoop/etc/hadoop/capacity-scheduler.xml
      - ./jobs:/workspace/jobs
      - worker3-data:/data/datanode

    depends_on:
      - master

    command: >
      bash -c "
        hdfs --daemon start datanode;
        yarn --daemon start nodemanager;
        tail -f /dev/null
      "

volumes:
  namenode-data:
  worker1-data:
  worker2-data:
  worker3-data:
```

Start everything:

```bash
docker compose up -d
```

Check:

```bash
docker compose ps
```

You should see:

```text
master
worker1
worker2
worker3
```

---

# 9. Inspect the Processes

Enter the master:

```bash
docker exec -it spark-yarn-master bash
```

Run:

```bash
jps
```

You should see processes including:

```text
NameNode
ResourceManager
```

Exit:

```bash
exit
```

Now inspect a worker:

```bash
docker exec -it spark-yarn-worker1 bash
```

```bash
jps
```

You should see:

```text
DataNode
NodeManager
```

That distinction is important.

The master is doing **coordination**.

Workers are doing **storage and computation**.

---

# 10. Inspect HDFS

Enter the master:

```bash
docker exec -it spark-yarn-master bash
```

Run:

```bash
hdfs dfsadmin -report
```

Look for:

```text
Live datanodes (3)
```

You should see:

```text
worker1
worker2
worker3
```

Look at:

```text
Configured Capacity
DFS Used
DFS Remaining
Live datanodes
```

You now have a miniature distributed filesystem.

---

# 11. HDFS Web UI

On your Mac, open:

```text
http://localhost:9870
```

This is the NameNode web UI.

Look around.

Find:

```text
Datanodes
Storage
Used capacity
Remaining capacity
Blocks
```

Pay particular attention to the DataNodes page.

You should have three nodes.

---

# 12. Inspect YARN

Inside the master:

```bash
yarn node -list
```

You should see something similar to:

```text
Total Nodes:3
```

Get more detail:

```bash
yarn node -list -all
```

Now run:

```bash
yarn top
```

Exit with:

```text
q
```

---

# 13. YARN ResourceManager UI

Open:

```text
http://localhost:8088
```

This is one of the most important admin tools in this lab.

Explore:

```text
Cluster
Nodes
Scheduler
Applications
```

Look at the total cluster resources.

You should see approximately:

```text
6 vcores
~9 GB YARN memory
```

There are currently no Spark jobs, so almost everything should be available.

---

# 14. Create HDFS Directories

Inside the master:

```bash
hdfs dfs -mkdir -p /data
hdfs dfs -mkdir -p /data/raw
hdfs dfs -mkdir -p /data/processed
hdfs dfs -mkdir -p /tmp
```

Check:

```bash
hdfs dfs -ls /
```

Then:

```bash
hdfs dfs -ls /data
```

---

# 15. Run Something Directly Against HDFS

Create a file:

```bash
echo "hello distributed world" > /tmp/hello.txt
```

Upload it:

```bash
hdfs dfs -put /tmp/hello.txt /data/raw/
```

Check:

```bash
hdfs dfs -ls /data/raw
```

Read it:

```bash
hdfs dfs -cat /data/raw/hello.txt
```

Now ask HDFS where the blocks live:

```bash
hdfs fsck /data/raw/hello.txt -files -blocks -locations
```

Look at the output.

Because:

```text
dfs.replication = 2
```

the block should have two replicas.

Look for two different DataNodes.

This is your first glimpse into **data locality**.

---

# 16. Generate Some Real Data

Now let's make a Spark job.

Create:

```text
jobs/generate_data.py
```

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder
    .appName("GenerateData")
    .getOrCreate()
)


rows = 5_000_000


df = (
    spark.range(rows)
    .withColumn("customer_id", F.col("id") % 100_000)
    .withColumn("product_id", F.col("id") % 10_000)
    .withColumn("region_id", F.col("id") % 20)
    .withColumn("amount", (F.col("id") % 500) + 1)
)


(
    df
    .repartition(24)
    .write
    .mode("overwrite")
    .parquet("hdfs:///data/raw/transactions")
)


spark.stop()
```

---

# 17. Submit Spark to YARN

Enter the master:

```bash
docker exec -it spark-yarn-master bash
```

Instead of:

```bash
python jobs/generate_data.py
```

use:

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 3 \
    --executor-cores 1 \
    --executor-memory 1g \
    jobs/generate_data.py
```

This is fundamentally different from the previous lab.

We are asking:

```text
YARN:
    Please run this Spark application.

Spark:
    I would like 3 executors.

Each executor:
    1 CPU core
    1 GB RAM.
```

YARN decides where they run.

---

# 18. Watch the Job

While it is running, open another terminal.

Run:

```bash
docker exec -it spark-yarn-master bash
```

Then:

```bash
yarn application -list
```

You should see something like:

```text
application_...
```

Save that ID mentally.

Then:

```bash
yarn application -status application_XXXXXXXXXXXX_0001
```

Look for:

```text
State
Final-State
Tracking-URL
Queue
User
Application-Type
```

---

# 19. Look at the Nodes

While Spark is running:

```bash
yarn node -list
```

Also check:

```bash
yarn top
```

You should see resources being consumed.

Compare:

```text
before Spark job

vs

during Spark job
```

This distinction is central to operating YARN.

---

# 20. Find the Spark Executors

Check each worker.

For example:

```bash
docker exec spark-yarn-worker1 jps
```

and:

```bash
docker exec spark-yarn-worker2 jps
```

and:

```bash
docker exec spark-yarn-worker3 jps
```

During a sufficiently long Spark application, you should see YARN container processes associated with your Spark executors.

The exact Java process names aren't as useful as the architectural idea:

```text
YARN NodeManager
        |
        +--- YARN container
               |
               +--- Spark executor
```

Spark didn't SSH into these machines.

Spark requested resources from YARN.

---

# 21. Inspect the Generated Data

After the job finishes:

```bash
hdfs dfs -du -h /data/raw/transactions
```

Then:

```bash
hdfs dfs -ls /data/raw/transactions
```

You'll see lots of:

```text
part-....snappy.parquet
```

Remember we explicitly used:

```python
.repartition(24)
```

That affects the number of output partitions/files.

---

# 22. Look at HDFS Block Distribution

Run:

```bash
hdfs fsck /data/raw/transactions -files -blocks -locations
```

Now you're looking at something much closer to a real distributed system:

```text
Parquet files
    |
    v
HDFS blocks
    |
    +--> worker1
    +--> worker2
    +--> worker3
```

with replicas distributed across machines.

---

# 23. Your First Cluster Query

Create:

```text
jobs/aggregate.py
```

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder
    .appName("AggregateTransactions")
    .getOrCreate()
)


df = spark.read.parquet(
    "hdfs:///data/raw/transactions"
)


result = (
    df
    .groupBy("region_id")
    .agg(
        F.count("*").alias("transactions"),
        F.sum("amount").alias("revenue"),
        F.avg("amount").alias("avg_amount"),
    )
    .orderBy("region_id")
)


result.show(100, truncate=False)


spark.stop()
```

Submit:

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 3 \
    --executor-cores 1 \
    --executor-memory 1g \
    jobs/aggregate.py
```

---

# 24. Stop and Think About the Architecture

Your application now roughly looks like:

```text
                        YARN ResourceManager
                               |
                        ApplicationMaster
                               |
              +----------------+----------------+
              |                |                |
              v                v                v
         Executor 1       Executor 2       Executor 3
          worker1          worker2          worker3
              |                |                |
              +-------+--------+--------+-------+
                      |
                    HDFS
```

Depending on scheduling, executors may not distribute perfectly one-per-worker.

That itself is something to investigate.

Run:

```bash
yarn application -list -appStates ALL
```

---

# 25. Client Mode vs Cluster Mode

So far we used:

```text
--deploy-mode cluster
```

Try:

```bash
spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 3 \
    --executor-cores 1 \
    --executor-memory 1g \
    jobs/aggregate.py
```

Compare the behavior.

Think about:

```text
Where is the Spark driver running?

cluster mode:
    driver runs inside the YARN application

client mode:
    driver runs inside the process where spark-submit was executed
```

For production batch jobs, cluster mode is generally the more interesting architecture.

---

# Part II — RESOURCE ADMINISTRATION

# 26. Understand Executor Sizing

Our cluster has:

```text
3 worker nodes

Each:

    3072 MB
    2 vcores
```

Therefore:

```text
Cluster:

    ~9 GB memory
    6 vcores
```

Now consider this Spark request:

```bash
--num-executors 3
--executor-cores 1
--executor-memory 1g
```

Roughly:

```text
executor 1 -> 1 core + memory
executor 2 -> 1 core + memory
executor 3 -> 1 core + memory
```

There is room for more.

Try:

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 6 \
    --executor-cores 1 \
    --executor-memory 768m \
    jobs/aggregate.py
```

Watch:

```bash
yarn top
```

and:

```text
http://localhost:8088
```

---

# 27. Oversubscribe the Cluster

Now deliberately request too much:

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 10 \
    --executor-cores 2 \
    --executor-memory 2g \
    jobs/aggregate.py
```

Watch YARN.

Don't immediately kill it.

Observe what happens.

Ask yourself:

```text
Did the application crash?

Or did YARN simply fail to give Spark every requested executor?
```

Look at:

```bash
yarn top
```

and:

```bash
yarn application -list
```

This distinction matters enormously when operating shared clusters.

---

# 28. Executor Memory Is Not the Whole Container

Spark executors also have **memory overhead**.

So this:

```text
--executor-memory 2g
```

does NOT necessarily mean:

```text
YARN container = exactly 2 GB
```

There is JVM heap plus container overhead and, for PySpark, Python memory considerations.

Look at the application's allocated memory through the YARN UI.

Compare it to your requested executor heap.

---

# 29. Try Different Executor Shapes

Run the same job with:

### Many small executors

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 6 \
    --executor-cores 1 \
    --executor-memory 768m \
    jobs/aggregate.py
```

Then:

### Fewer larger executors

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 3 \
    --executor-cores 2 \
    --executor-memory 1536m \
    jobs/aggregate.py
```

Compare:

```text
runtime
executor count
task concurrency
resource utilization
```

There is no universal "best executor size."

That's part of cluster tuning.

---

# Part III — SPARK QUERY OPTIMIZATION

# 30. Understand Shuffles

This query:

```python
df.groupBy("region_id").count()
```

requires records with the same:

```text
region_id
```

to eventually meet at the same partition.

That means data moves across executors.

Conceptually:

```text
BEFORE

Executor 1:
    region 1
    region 7
    region 3

Executor 2:
    region 7
    region 2
    region 1

Executor 3:
    region 1
    region 3
    region 7


            SHUFFLE


AFTER

Partition A:
    all region 1

Partition B:
    all region 2

Partition C:
    all region 3

...
```

Network transfer is one of the most important costs in Spark.

---

# 31. Control Shuffle Partitions

Spark SQL has:

```text
spark.sql.shuffle.partitions
```

Try:

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 3 \
    --executor-cores 2 \
    --executor-memory 1536m \
    --conf spark.sql.shuffle.partitions=2 \
    jobs/aggregate.py
```

Then:

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 3 \
    --executor-cores 2 \
    --executor-memory 1536m \
    --conf spark.sql.shuffle.partitions=200 \
    jobs/aggregate.py
```

Then:

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 3 \
    --executor-cores 2 \
    --executor-memory 1536m \
    --conf spark.sql.shuffle.partitions=20 \
    jobs/aggregate.py
```

Compare them.

Think about the extremes.

Too few partitions:

```text
large tasks
poor parallelism
possibly large memory requirements
```

Too many:

```text
tiny tasks
scheduler overhead
many tiny shuffle files
```

---

# 32. Inspect the Physical Query Plan

Modify:

```text
aggregate.py
```

before:

```python
result.show()
```

add:

```python
result.explain("formatted")
```

Submit again.

Look for words such as:

```text
Exchange
HashAggregate
Sort
```

An:

```text
Exchange
```

is a strong hint that data is being shuffled.

---

# 33. Create a Join Workload

Create:

```text
jobs/joins.py
```

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder
    .appName("JoinExperiment")
    .getOrCreate()
)


transactions = spark.read.parquet(
    "hdfs:///data/raw/transactions"
)


products = (
    spark.range(10_000)
    .withColumnRenamed("id", "product_id")
    .withColumn(
        "category",
        F.concat(
            F.lit("category-"),
            (F.col("product_id") % 100)
        )
    )
)


result = (
    transactions
    .join(products, "product_id")
    .groupBy("category")
    .agg(
        F.sum("amount").alias("revenue")
    )
)


result.explain("formatted")

result.show(100)


spark.stop()
```

Submit it:

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 3 \
    --executor-cores 2 \
    --executor-memory 1536m \
    jobs/joins.py
```

Look at the join strategy.

---

# 34. Disable Automatic Broadcast Joins

Submit:

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 3 \
    --executor-cores 2 \
    --executor-memory 1536m \
    --conf spark.sql.autoBroadcastJoinThreshold=-1 \
    jobs/joins.py
```

Inspect:

```text
explain("formatted")
```

Look for something like:

```text
SortMergeJoin
```

and:

```text
Exchange
```

Now compare that to the default plan.

---

# 35. Explicitly Broadcast the Small Table

Modify the join:

```python
result = (
    transactions
    .join(
        F.broadcast(products),
        "product_id"
    )
    .groupBy("category")
    .agg(
        F.sum("amount").alias("revenue")
    )
)
```

Run again.

Look for:

```text
BroadcastHashJoin
```

Conceptually:

```text
Normal distributed join

large table -------- shuffle -----\
                                   JOIN
small table -------- shuffle -----/


Broadcast join

                         +--> executor 1
small table -> broadcast +--> executor 2
                         +--> executor 3

large table stays distributed
```

Broadcasting a genuinely small table can eliminate a major shuffle.

---

# 36. Observe Adaptive Query Execution

Check:

```python
print(
    spark.conf.get(
        "spark.sql.adaptive.enabled"
    )
)
```

Spark's Adaptive Query Execution can modify parts of a query plan based on runtime information.

Try explicitly disabling it:

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 3 \
    --executor-cores 2 \
    --executor-memory 1536m \
    --conf spark.sql.adaptive.enabled=false \
    jobs/joins.py
```

Then:

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 3 \
    --executor-cores 2 \
    --executor-memory 1536m \
    --conf spark.sql.adaptive.enabled=true \
    jobs/joins.py
```

Compare the physical plans.

---

# Part IV — DATA SKEW

# 37. Create Horribly Skewed Data

Create:

```text
jobs/skew.py
```

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder
    .appName("SkewExperiment")
    .getOrCreate()
)


df = spark.range(10_000_000)


df = df.withColumn(
    "customer_id",
    F.when(
        F.col("id") < 9_000_000,
        F.lit(1)
    ).otherwise(
        F.col("id")
    )
)


result = (
    df
    .groupBy("customer_id")
    .count()
)


result.explain("formatted")

result.write.mode("overwrite").parquet(
    "hdfs:///data/processed/skew-result"
)


spark.stop()
```

Here:

```text
90% of all rows

have:

customer_id = 1
```

This is intentionally awful.

---

# 38. Run the Skewed Query

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 3 \
    --executor-cores 2 \
    --executor-memory 1536m \
    --conf spark.sql.shuffle.partitions=20 \
    jobs/skew.py
```

Think about the shuffle.

Most partitions might finish quickly.

One partition may receive:

```text
9,000,000 rows
```

That single task can become the bottleneck.

This is the classic pattern:

```text
Task 1  ███
Task 2  ██
Task 3  ████
Task 4  █
Task 5  ███████████████████████████████
```

More machines do not automatically solve this problem.

---

# 39. Why "Add More Executors" Doesn't Always Work

Imagine:

```text
99 tasks complete in 2 seconds

1 task takes 2 minutes
```

You could double your cluster size.

The slow task still has to execute somewhere.

This is an extremely important distributed systems lesson:

```text
cluster resources != automatic scalability
```

Partitioning matters.

Data distribution matters.

Query structure matters.

---

# Part V — YARN ADMINISTRATION

# 40. Add Multiple Queues

Now we'll pretend two teams share this cluster:

```text
analytics
research
```

Change:

```text
config/capacity-scheduler.xml
```

to:

```xml
<?xml version="1.0"?>
<configuration>

    <property>
        <name>yarn.scheduler.capacity.root.queues</name>
        <value>analytics,research</value>
    </property>


    <!-- Analytics -->

    <property>
        <name>yarn.scheduler.capacity.root.analytics.capacity</name>
        <value>70</value>
    </property>

    <property>
        <name>yarn.scheduler.capacity.root.analytics.maximum-capacity</name>
        <value>100</value>
    </property>


    <!-- Research -->

    <property>
        <name>yarn.scheduler.capacity.root.research.capacity</name>
        <value>30</value>
    </property>

    <property>
        <name>yarn.scheduler.capacity.root.research.maximum-capacity</name>
        <value>100</value>
    </property>

</configuration>
```

We now conceptually have:

```text
root
├── analytics
│      guaranteed capacity: 70%
│
└── research
       guaranteed capacity: 30%
```

Restart the ResourceManager:

```bash
docker restart spark-yarn-master
```

Give the cluster a moment to reconnect.

Check:

```bash
docker exec -it spark-yarn-master bash
```

Then:

```bash
yarn node -list
```

Open:

```text
http://localhost:8088
```

Go to:

```text
Scheduler
```

Inspect your queues.

---

# 41. Submit to a Specific Queue

Submit:

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --queue analytics \
    --num-executors 3 \
    --executor-cores 1 \
    --executor-memory 1g \
    jobs/aggregate.py
```

Now:

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --queue research \
    --num-executors 2 \
    --executor-cores 1 \
    --executor-memory 1g \
    jobs/aggregate.py
```

Look at:

```bash
yarn application -list
```

and the Scheduler UI.

---

# 42. Why Queues Matter

Imagine your real cluster has:

```text
300 analytics scripts

all starting at midnight
```

Without workload management:

```text
300 apps
  |
  v
all request resources simultaneously
```

With YARN scheduling:

```text
                     YARN
                      |
           +----------+----------+
           |                     |
      analytics queue       critical queue
           |                     |
       many jobs             important jobs
```

The cluster manager decides how much capacity different workloads receive.

This is one of the big reasons a cluster manager exists.

---

# Part VI — DYNAMIC ALLOCATION

# 43. Try Dynamic Allocation

So far we've explicitly said:

```text
--num-executors 3
```

But Spark can dynamically adjust executor counts.

Try:

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=1 \
    --conf spark.dynamicAllocation.initialExecutors=1 \
    --conf spark.dynamicAllocation.maxExecutors=6 \
    --executor-cores 1 \
    --executor-memory 1g \
    jobs/aggregate.py
```

The intended behavior is now roughly:

```text
start:
    1 executor

demand increases:
    2
    3
    4
    ...

idle:
    remove executors
```

Instead of reserving a fixed executor count for the lifetime of the application.

Watch:

```bash
yarn top
```

and:

```text
http://localhost:8088
```

For very short jobs you may not see dramatic scaling, so increasing the generated dataset or adding a more expensive workload can make the behavior easier to observe.

---

# Part VII — CLUSTER FAILURE

# 44. Kill a Worker

Check the healthy cluster first:

```bash
hdfs dfsadmin -report
```

and:

```bash
yarn node -list
```

You should have:

```text
3 DataNodes
3 NodeManagers
```

Now from your Mac:

```bash
docker stop spark-yarn-worker3
```

Wait a little and check:

```bash
docker exec -it spark-yarn-master bash
```

```bash
yarn node -list -all
```

Then:

```bash
hdfs dfsadmin -report
```

Eventually the cluster should realize that worker3 is unavailable.

---

# 45. Can You Still Read Your Data?

Try:

```bash
hdfs dfs -ls /data/raw/transactions
```

Then run:

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 2 \
    --executor-cores 1 \
    --executor-memory 1g \
    jobs/aggregate.py
```

The cluster now has fewer resources.

But because HDFS replicated blocks, much of your data should still be available.

This is one major distinction between:

```text
distributed storage
```

and:

```text
a network-mounted folder
```

---

# 46. Look for Under-Replicated Blocks

Run:

```bash
hdfs fsck /data
```

Look for:

```text
under-replicated blocks
```

Remember:

```text
replication factor = 2
```

If one of two copies lived on worker3, HDFS now needs another copy somewhere else.

---

# 47. Bring the Worker Back

On your Mac:

```bash
docker start spark-yarn-worker3
```

Then:

```bash
docker exec -it spark-yarn-master bash
```

Check:

```bash
yarn node -list
```

and:

```bash
hdfs dfsadmin -report
```

Your cluster should eventually return to three workers.

---

# Part VIII — SCALE THE CLUSTER

# 48. Convert Workers Into a Scalable Service

At this point, our Compose file has:

```text
worker1
worker2
worker3
```

which is intentionally explicit because it made the architecture easy to understand.

A more dynamic setup would use a generic:

```text
worker
```

service and scale it.

You don't have to change the lab yet, but understand the goal:

```bash
docker compose up --scale worker=5
```

would conceptually add NodeManagers/DataNodes.

Then YARN would see more resources:

```text
3 workers:

6 cores
9 GB


5 workers:

10 cores
15 GB
```

This is approximately the distinction between:

```text
Spark configuration
```

and:

```text
cluster provisioning
```

Spark does not create the underlying machines.

The infrastructure layer creates machines.

YARN discovers/manages their usable compute resources.

Spark consumes those resources.

---

# Part IX — ADMIN COMMAND CHEAT SHEET

# 49. HDFS Commands

Cluster health:

```bash
hdfs dfsadmin -report
```

List files:

```bash
hdfs dfs -ls /
```

Recursive list:

```bash
hdfs dfs -ls -R /data
```

Disk usage:

```bash
hdfs dfs -du -h /data
```

Upload:

```bash
hdfs dfs -put local-file /data/
```

Download:

```bash
hdfs dfs -get /data/file .
```

Delete:

```bash
hdfs dfs -rm /data/file
```

Recursive delete:

```bash
hdfs dfs -rm -r /data/path
```

Inspect blocks:

```bash
hdfs fsck /data -files -blocks -locations
```

---

# 50. YARN Commands

List nodes:

```bash
yarn node -list
```

All nodes:

```bash
yarn node -list -all
```

Active applications:

```bash
yarn application -list
```

All applications:

```bash
yarn application -list -appStates ALL
```

Application status:

```bash
yarn application -status APPLICATION_ID
```

Kill application:

```bash
yarn application -kill APPLICATION_ID
```

Terminal resource monitor:

```bash
yarn top
```

Logs:

```bash
yarn logs -applicationId APPLICATION_ID
```

---

# 51. Spark Submission Commands

Minimal:

```bash
spark-submit \
    --master yarn \
    jobs/aggregate.py
```

Cluster mode:

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    jobs/aggregate.py
```

Fixed executors:

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 3 \
    --executor-cores 2 \
    --executor-memory 1536m \
    jobs/aggregate.py
```

Queue:

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --queue analytics \
    jobs/aggregate.py
```

Configuration override:

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.sql.shuffle.partitions=20 \
    jobs/aggregate.py
```

---

# Part X — THINGS TO EXPERIMENT WITH

At this point, stop following the lab rigidly.

Start breaking things.

---

## Experiment 1 — One Giant Executor

Try something like:

```text
1 executor
2 cores
2 GB memory
```

Compare it against:

```text
3 executors
1 core each
1 GB each
```

Think about:

```text
parallelism
failure blast radius
memory
GC
resource fragmentation
```

---

## Experiment 2 — Tons of Tiny Executors

Try maximizing the number of tiny executors.

Observe whether performance actually improves.

---

## Experiment 3 — Ridiculous Shuffle Partition Count

Try:

```text
spark.sql.shuffle.partitions=1
```

Then:

```text
10
```

Then:

```text
100
```

Then:

```text
1000
```

Look at task count and runtime.

---

## Experiment 4 — Large vs Small Files

Generate:

```text
5 partitions
```

then:

```text
500 partitions
```

Write both to HDFS.

Compare:

```bash
hdfs dfs -ls
```

Think about the classic:

```text
small files problem
```

---

## Experiment 5 — Broadcast Joins

Create:

```text
10 million transaction rows

and

100 product rows
```

Compare:

```text
SortMergeJoin
```

against:

```text
BroadcastHashJoin
```

---

## Experiment 6 — Two Spark Applications Simultaneously

Terminal 1:

```bash
spark-submit ...
```

Terminal 2:

```bash
spark-submit ...
```

Watch:

```bash
yarn top
```

See how YARN divides resources.

---

## Experiment 7 — Ten Applications Simultaneously

Now you're getting closer to the workload-management problem.

Launch many copies.

Observe:

```text
running apps
pending apps
allocated resources
containers
queues
```

---

## Experiment 8 — Kill an Executor's Worker

Start a long Spark application.

Then:

```bash
docker stop spark-yarn-worker2
```

See what Spark and YARN do.

Look at:

```bash
yarn logs -applicationId ...
```

Then bring it back:

```bash
docker start spark-yarn-worker2
```

---

## Experiment 9 — Kill the ResourceManager

Try:

```bash
docker stop spark-yarn-master
```

What happens?

This exposes another production concern:

```text
ResourceManager High Availability
```

Our little cluster doesn't have HA.

A real production cluster may have:

```text
Active ResourceManager
Standby ResourceManager
```

Likewise, production HDFS often uses NameNode HA.

---

# Part XI — HOW TO THINK ABOUT PERFORMANCE

When a Spark job is slow, don't immediately think:

```text
"We need more machines."
```

Start with:

```text
1. Is CPU saturated?

2. Is memory exhausted?

3. Is the cluster actually fully utilized?

4. Are executors waiting for resources?

5. Is one task dramatically slower than others?

6. Is there data skew?

7. Is there a large shuffle?

8. Are we spilling to disk?

9. Are we doing a SortMergeJoin when we could broadcast?

10. Are there too many/few partitions?

11. Are we repeatedly reading the same data?

12. Are there millions of tiny files?

13. Is serialization expensive?

14. Are executors dying?

15. Is the driver the bottleneck?
```

A useful mental model is:

```text
                   ┌─────────────┐
                   │    Query    │
                   └──────┬──────┘
                          │
                          v
                   ┌─────────────┐
                   │ Spark plan  │
                   └──────┬──────┘
                          │
              tasks / stages / shuffle
                          │
                          v
                ┌───────────────────┐
                │ Spark executors   │
                └────────┬──────────┘
                         │
                  containers requested
                         │
                         v
                ┌───────────────────┐
                │       YARN        │
                │ ResourceManager   │
                └────────┬──────────┘
                         │
                  allocates resources
                         │
          +--------------+--------------+
          |              |              |
          v              v              v
      worker1        worker2        worker3
          |              |              |
          +--------------+--------------+
                         |
                         v
                       HDFS
```

Each layer has different knobs.

---

# Part XII — WHO CONTROLS WHAT?

This is worth memorizing.

## Infrastructure

Responsible for:

```text
machines
CPUs
RAM
disks
network
```

Examples:

```text
physical servers
VMs
cloud instances
Kubernetes nodes
```

---

## HDFS

Responsible for:

```text
distributed storage
block placement
replication
storage failure recovery
```

Components:

```text
NameNode
DataNode
```

---

## YARN

Responsible for:

```text
cluster compute resources
application scheduling
container allocation
queues
resource isolation
```

Components:

```text
ResourceManager
NodeManager
ApplicationMaster
Container
```

---

## Spark

Responsible for:

```text
query planning
stages
tasks
executors
shuffles
joins
caching
data processing
```

Components:

```text
Driver
Executors
Tasks
Stages
```

---

# Part XIII — RELATING THIS TO A REAL ON-PREM CLUSTER

Suppose you had:

```text
20 physical servers
```

Each:

```text
32 CPU cores
256 GB RAM
10 TB disk
```

You might install:

```text
DataNode
NodeManager
```

on every worker.

So:

```text
                        master servers

                    NameNode
                    ResourceManager
                         |
       +-----------------+--------------------+
       |                 |                    |
       v                 v                    v
    server1           server2               ...
   DataNode          DataNode
 NodeManager       NodeManager
       |                 |
       v                 v
 Spark Executor      Spark Executor
```

When new hardware arrives:

```text
rack server

install Linux

install Hadoop

configure HDFS

configure YARN

start DataNode

start NodeManager
```

The server joins the cluster.

HDFS gains storage capacity.

YARN gains compute capacity.

Spark can then consume the new YARN resources.

---

# Part XIV — THE CONNECTION TO KUBERNETES

Once this architecture makes sense, Kubernetes becomes much easier to reason about.

Rough approximation:

```text
YARN                        Kubernetes

ResourceManager      ~      scheduler/control plane

NodeManager          ~      kubelet

YARN container       ~      pod/container resources

YARN queue           ~      queue/resource-policy layer

Worker node          ~      Kubernetes node
```

These aren't exact one-to-one equivalents, but the comparison is useful.

Spark can effectively say:

```text
Give me:

4 executors
2 cores each
4 GB each
```

to either a cluster manager such as:

```text
YARN
```

or:

```text
Kubernetes
```

The cluster manager figures out where those processes actually run.

That means there are really two different questions:

```text
How does Spark distribute computation?

and

How does the infrastructure distribute workloads?
```

Spark answers the first.

YARN/Kubernetes help answer the second.

---

# Part XV — TEARDOWN

When you're done but want to keep HDFS data:

```bash
docker compose down
```

Start again later:

```bash
docker compose up -d
```

Because we used Docker volumes, your HDFS state should remain.

To destroy absolutely everything:

```bash
docker compose down -v
```

Then:

```bash
docker image rm spark-yarn-lab
```

if you also want to remove the image.

---

# Final Challenges

At this point you should be able to attempt these without following step-by-step instructions.

### Challenge 1

Create a Spark job requiring roughly:

```text
4 GB
4 cores
```

of cluster resources.

Determine how YARN places its executors.

---

### Challenge 2

Launch four Spark applications simultaneously.

Configure YARN so:

```text
critical jobs
```

are guaranteed at least:

```text
50%
```

of cluster resources.

---

### Challenge 3

Construct a join where:

```text
table A = millions of rows
table B = hundreds of rows
```

Run it:

```text
without broadcast
```

and:

```text
with broadcast
```

Explain the physical plan difference.

---

### Challenge 4

Create intentionally skewed data where:

```text
95%
```

of records share one key.

Identify the slow partition.

Find at least one way to mitigate it.

---

### Challenge 5

Run a long Spark application.

Kill one worker.

Determine:

```text
what YARN notices
what Spark notices
what HDFS notices
```

These are three different systems reacting to the same machine failure.

---

### Challenge 6

Increase:

```text
dfs.replication
```

from:

```text
2
```

to:

```text
3
```

Determine what happens to existing files versus newly written files.

Use:

```bash
hdfs fsck
```

to verify your assumptions.

---

### Challenge 7

Determine the best executor layout for this cluster.

Compare at least:

```text
6 × 1-core executors

3 × 2-core executors

2 × 2-core executors

1 × 2-core executor
```

Do not judge only by runtime.

Also consider:

```text
cluster utilization
task concurrency
memory
container allocation
failure behavior
```

---

### Challenge 8

Pretend these are your overnight analytics workloads:

```text
job-001
job-002
...
job-300
```

Design a YARN configuration where all 300 can be submitted simultaneously without being allowed to consume the entire cluster uncontrollably.

Think about:

```text
queues
capacities
executor limits
dynamic allocation
job concurrency
Airflow pools
priority
```

This is now much closer to an actual platform-engineering problem than a Spark programming exercise.

---

# What You Should Understand After This Lab

You don't need to memorize every Hadoop configuration parameter.

You should instead walk away understanding this hierarchy:

```text
                    PHYSICAL / VIRTUAL MACHINES
                              |
                              v
                     +------------------+
                     |      HADOOP      |
                     |                  |
                     | HDFS        YARN |
                     +---+-----------+--+
                         |           |
                    storage      resources
                         |           |
                         +-----+-----+
                               |
                               v
                         +-----------+
                         |   SPARK   |
                         |           |
                         | driver    |
                         | executors |
                         | tasks     |
                         +-----------+
                               |
                               v
                         your query
```

And when somebody says:

> "The Spark cluster is overloaded."

you should now start asking:

```text
What exactly is overloaded?

YARN memory?

YARN CPU?

HDFS?

Network?

Executors?

Driver?

Shuffle?

A single skewed partition?

Too many concurrent applications?

A scheduler queue?

Bad executor sizing?

Bad query planning?
```

That is the transition from **using Spark** to **operating Spark**.
