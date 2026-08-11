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