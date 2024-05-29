import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
logging.basicConfig(level = logging.INFO)

spark = SparkSession \
    .builder \
    .appName("Read csv files") \
    .master("local[*]") \
    .getOrCreate()

df = spark \
    .read \
    .format("csv") \
    .option("header", "true") \
    .load("./data/zipcodes.csv")

df.show()


df_options = spark \
    .read \
    .format("csv") \
    .options(header=True, inferSchema=True, delimiter=",", quotes='"') \
    .load("./data/zipcodes.csv")

df_options.show()

spark.stop()