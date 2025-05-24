from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("Nested JSON Parser") \
    .master("local[*]") \
    .getOrCreate()

json_path = "../data/nested_json.json"

df = spark.read.json(json_path)

df.printSchema()

df.show(truncate=False)

# get the column values from df
df.withColumn("array_struct_a", F.col("array_struct").getItem("a")).show()

df2 = df.withColumn("dates", F.split(F.col("date_hour"), " ")[0])

df2.groupBy("dates") \
    .agg(F.sum(F.col("visit_data").getItem("ios")).alias("ios_cnt"),
    F.sum(F.col("visit_data").getItem("android")).alias("android_cnt"),
    F.sum(F.col("visit_data").getItem("web")).alias("web_cnt")) \
    .fillna(0, subset=["ios_cnt", "android_cnt", "web_cnt"]) \
    .show()

spark.stop()

