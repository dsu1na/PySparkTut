from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import time

## When cached data storage size is 3 MiB when using Kryo Serializer
## Same 3MiB with deafult serializer

# spark = SparkSession \
#     .builder \
#     .appName("serializer app") \
#     .master("local[*]") \
#     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
#     .getOrCreate()

spark = SparkSession \
    .builder \
    .appName("serializer app") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.format("parquet").load("../data/covid_tracking.parquet")
df.cache()

df.withColumn("extra", F.lit("default column")).show()

time.sleep(60)
spark.stop()