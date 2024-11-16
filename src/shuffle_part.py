import time

from pyspark.sql import SparkSession
import pyspark.sql.functions as F 

# spark = SparkSession.builder \
#     .appName("shuffle partitons app") \
#     .master("local[*]") \
#     .getOrCreate()

spark = SparkSession \
    .builder \
    .appName("shuffle partitons app") \
    .master("local-cluster[2,4,2048]") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.cores", "2") \
    .getOrCreate()

spark.sparkContext.setJobDescription("Read Parquet")

df = spark.read.format("parquet").load("../data/covid_tracking.parquet")

# df.show(5)
# df.printSchema()

spark.sparkContext.setJobDescription("Show top rows of parquet read")
df.select(F.sum(F.col("negative_increase"))).show()
time.sleep(120)
spark.stop()