from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import time
import datetime
from pyspark import StorageLevel

## When cached data storage size is 3 MiB when using Kryo Serializer
## Same 3MiB with deafult serializer

spark = SparkSession \
    .builder \
    .appName("serializer app") \
    .master("local[*]") \
    .getOrCreate()

# spark = SparkSession \
#     .builder \
#     .appName("serializer app") \
#     .master("local[*]") \
#     .getOrCreate()

# sc = spark.sparkContext
# sc.setSystemProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
# sc.setSystemProperty("setWarnUnregisteredClasses","true")
# sc.setSystemProperty("spark.kryo.registrationRequired", "true")

# configurations = spark.sparkContext.getConf().get("spark.serializer")
# print(configurations)

print(f"Time of reading parquet file: {datetime.datetime.now()}")

df = spark.read.format("parquet").load("../data/covid_tracking.parquet")
df.persist(StorageLevel.MEMORY_ONLY)

df.withColumn("extra", F.lit("default column")).show()

print(f"Time of completion of transformations: {datetime.datetime.now()}")
time.sleep(60)

spark.stop()