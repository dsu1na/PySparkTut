from pyspark.sql import SparkSession
import pyspark.sql.functions as F 

spark = SparkSession \
    .builder \
    .appName("Aggregation App") \
    .master("local[*]") \
    .getOrCreate()

simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
]

schema = ["employee_name", "department", "state", "salary", "age", "bonus"]

df = spark.createDataFrame(data = simpleData, schema = schema)
df.printSchema()

df \
    .groupBy(F.col("department")) \
    .sum("bonus", "salary") \
    .show()

df \
    .groupBy(F.col("department")) \
    .count().alias("departmentWiseCount") \
    .show()

df \
    .groupBy("department") \
    .agg(F.sum(F.col("salary")).alias("sum_salary"), \
        F.avg(F.col("salary")).alias("avg_salary"), \
        F.sum(F.col("bonus")).alias("sum_bonus"), \
        F.avg(F.col("bonus")).alias("avg_bonus")
    ) \
    .show()

df \
    .groupBy("department") \
    .agg(F.sum(F.col("salary")).alias("sum_salary"), \
        F.avg(F.col("salary")).alias("avg_salary"), \
        F.sum(F.col("bonus")).alias("sum_bonus"), \
        F.avg(F.col("bonus")).alias("avg_bonus")
    ) \
    .where(F.col("sum_bonus") >= 50000) \
    .show()

df \
    .where(F.col("state") != "NY") \
    .groupBy("department") \
    .agg(F.sum(F.col("salary")).alias("sum_salary"), \
        F.avg(F.col("salary")).alias("avg_salary"), \
        F.sum(F.col("bonus")).alias("sum_bonus"), \
        F.avg(F.col("bonus")).alias("avg_bonus")
    ) \
    .show()