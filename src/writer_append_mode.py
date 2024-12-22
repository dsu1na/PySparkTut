from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import time
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level = logging.INFO)

spark = SparkSession \
    .builder \
    .appName("append mode spark program") \
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
df.write.mode("append").partitionBy("year").format("parquet").save("/PySparkTut/dest_data/append_data/")

logger.info("Reading 2021 partition ...")
spark.read.format("parquet").load("/PySparkTut/dest_data/append_data/year=2021").show()

logger.info("Reading 2022 partition ...")
spark.read.format("parquet").load("/PySparkTut/dest_data/append_data/year=2022").show()

logger.info("Appending new data to 2021 partition ...")
new_data = [
    ("Raj", "2021")
]

new_df = spark.createDataFrame(data=new_data, schema=dataframe_schema)
new_df.write.mode("append").partitionBy("year").format("parquet").save("/PySparkTut/dest_data/append_data/")

logger.info("Reading 2021 partition after appending ...")
spark.read.format("parquet").load("/PySparkTut/dest_data/append_data/year=2021").show()

logger.info("Reading 2022 partition after appending new data ...")
spark.read.format("parquet").load("/PySparkTut/dest_data/append_data/year=2022").show()

spark.stop()