from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
import time
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level = logging.INFO)

spark = SparkSession \
    .builder \
    .appName("dynamic overwrite mode spark program") \
    .master("local[2]") \
    .getOrCreate()

df = spark \
    .read \
    .format("csv") \
    .options(header=True, inferSchema=True, delimiter=",", quotes='"') \
    .load("/PySparkTut/data/zipcodes.csv")

logger.info("Caching the original dataframe ...")

df_agg = df.groupBy("State").agg(F.sum("TotalWages").alias("SumWages"))
df_agg.cache()
df_agg.createOrReplaceTempView("cached_view")
df_agg.write.format("noop").mode("overwrite").save()

logger.info("Caching of original dataframe completed ...")

logger.info("Reading from cache ...")
df2 = spark.sql("select * from cached_view").filter(F.col("State") == "PR")
df2.write.format("noop").mode("overwrite").save()


time.sleep(200)

spark.stop()