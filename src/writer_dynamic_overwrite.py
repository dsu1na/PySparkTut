from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import time
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level = logging.INFO)

# spark = SparkSession \
#     .builder \
#     .appName("dynamic overwrite mode spark program") \
#     .master("local[2]") \
#     .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
#     .getOrCreate()

spark = SparkSession \
    .builder \
    .appName("dynamic overwrite mode spark program") \
    .master("local[2]") \
    .getOrCreate()

data = [
    ("Ram", "2021"),
    ("Sham", "2022")
]

dataframe_schema = T.StructType([
    T.StructField("name", T.StringType(), True),
    T.StructField("year", T.StringType(), False)
])

df = spark.createDataFrame(data=data, schema=dataframe_schema)
df.show()
df.write.mode("overwrite").partitionBy("year").format("parquet").save("/PySparkTut/dest_data/dyn_overwrite_data/")

logger.info("Query the table for data ...")
spark.read.format("parquet").load("/PySparkTut/dest_data/dyn_overwrite_data/").show()

logger.info("Overwrite with a new partition 2024 ...")
new_data = [
    ("Raju", "2024"),
    ("Kachra", "2025")
]

new_df = spark.createDataFrame(data = new_data, schema = dataframe_schema)
new_df.write.mode("overwrite").partitionBy("year").format("parquet").save("/PySparkTut/dest_data/dyn_overwrite_data/")

logger.info("Query the table after adding 2024 and 2025 partition ...")
spark.read.format("parquet").load("/PySparkTut/dest_data/dyn_overwrite_data/").show()

logger.info("set the partition overwrite config to dynamic ...")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

logger.info("Overwrite in dynamic mode ...")

dyn_data = [
    ("ABC", "2024"),
    ("DCE", "2024")
]

dyn_df = spark.createDataFrame(data = dyn_data, schema = dataframe_schema)
dyn_df.write.mode("overwrite").partitionBy("year").save("/PySparkTut/dest_data/dyn_overwrite_data/")

logger.info("Query the table for data ...")
spark.read.format("parquet").load("/PySparkTut/dest_data/dyn_overwrite_data/").show()

logger.info("set the partition overwrite mode to static")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

static_data = [
    ("Seth", "2025")
]

static_df = spark.createDataFrame(data = static_data, schema = dataframe_schema)
static_df.write.mode("overwrite").partitionBy("year").format("parquet").save("/PySparkTut/dest_data/dyn_overwrite_data/")

logger.info("Query the table for data ...")
spark.read.format("parquet").load("/PySparkTut/dest_data/dyn_overwrite_data/").show()

spark.stop()