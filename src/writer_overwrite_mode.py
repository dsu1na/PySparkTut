from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import time
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level = logging.INFO)

spark = SparkSession \
    .builder \
    .appName("overwrite mode spark program") \
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
df.write.partitionBy("year").mode("overwrite").format("parquet").save("/PySparkTut/dest_data/overwrite_data/")

logger.info("create new dataframe with one partition 2024 ..")
new_data = [("Raju", "2024")]

new_df = spark.createDataFrame(data=new_data, schema=dataframe_schema)
new_df.write.partitionBy("year").mode("overwrite").format("parquet").save("/PySparkTut/dest_data/overwrite_data/")  # overwrite the existing data


logger.info("reading from destition data ..")

spark.read.format("parquet").load("/PySparkTut/dest_data/overwrite_data/").show()

spark.stop()