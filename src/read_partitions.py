import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import time

logger = logging.getLogger(__name__)
logging.basicConfig(level = logging.INFO)

spark = SparkSession \
    .builder \
    .appName("Read csv files") \
    .master("local[*]") \
    .config("spark.sql.files.maxPartitionBytes", "499870b") \
    .getOrCreate()

# spark = SparkSession \
#     .builder \
#     .appName("Read csv files") \
#     .master("local-cluster[2,4,2048]") \
#     .config("spark.executor.cores", "2") \
#     .config("spark.driver.cores", "2") \
#     .getOrCreate()

# df = spark \
#     .read \
#     .format("csv") \
#     .option("header", "true") \
#     .load("./data/zipcodes.csv")

# size of covid file in bytes = 1999480.  /4 = 499870
df = spark.read.format("parquet").load("./data/covid_tracking.parquet")

# df = df.filter(F.col("State") == "TX")
# df.count()
print(df.rdd.getNumPartitions())
print(spark.sparkContext.defaultParallelism)

# sc = spark.sparkContext
# print(sc.defaultParallelism)

# time.sleep(120)
spark.stop()