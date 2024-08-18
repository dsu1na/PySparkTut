from pyspark import SparkContext, MarshalSerializer, StorageLevel
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import time

spark_context = SparkContext(serializer=MarshalSerializer())
spark = SparkSession \
    .builder \
    .appName("marshal ser app") \
    .master("local[*]") \
    .getOrCreate()


df = spark.read.format("parquet").load("../data/covid_tracking.parquet")
df.persist(StorageLevel.MEMORY_ONLY)

df.withColumn("extra", F.lit("default column")).show()


time.sleep(60)

spark.stop()