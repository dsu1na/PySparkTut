import logging
from pyspark.sql import SparkSession
import time

logger = logging.getLogger(__name__)
logging.basicConfig(level = logging.INFO)


spark = SparkSession \
    .builder \
    .appName("Read csv files") \
    .master("local[*]") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "1g") \
    .config("spark.driver.cores", "1") \
    .getOrCreate()

df = spark \
    .read \
    .format("csv") \
    .option("header", "true") \
    .load("./data/zipcodes.csv")

print(df.rdd.getNumPartitions())


# df_options = spark \
#     .read \
#     .format("csv") \
#     .options(header=True, inferSchema=True, delimiter=",", quotes='"') \
#     .load("./data/zipcodes.csv")

# df_options.show()

time.sleep(60)
spark.stop()