from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
import time

spark = SparkSession \
    .builder \
    .appName("temp view") \
    .master("local[*]") \
    .getOrCreate()

"""
This part is for checking if createOrReplaceTempView is lazily evaluted

- It was found the it was lazily eveluated
"""
# df = spark.read.format("parquet").load("../data/covid_tracking.parquet")

# df.createOrReplaceTempView("sample_table")

# df.show()

"""
Function return if lazily evaluated
"""
df = spark.read.format("parquet").load("../data/covid_tracking.parquet")

def create_new_col(dataframe):
    dataframe = dataframe \
        .withColumn("dummy_col", F.lit("TBD")) \
        .select(F.col("date"), F.col("dummy_col"))

    return dataframe


df = create_new_col(df)

df.show()

time.sleep(60)
spark.stop()