from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# create sparksession 

spark = SparkSession \
    .builder \
    .appName("Window function app") \
    .master("local[*]") \
    .getOrCreate()

simpleData = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100) \
  )

schema = StructType([
    StructField("employee_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True)
])

df = spark.createDataFrame(data = simpleData, schema = schema)
df.show()
df.printSchema()

windowSpec = Window.partitionBy(F.col("department")).orderBy(F.col("salary"))

df_window = df.withColumn("row_number", \
    F.row_number().over(windowSpec))

df_window.show()

row_between_spec = Window.partitionBy(F.col("department")) \
    .orderBy(F.col("salary")) \
    .rowsBetween(-2, Window.currentRow)

df_row_bt = df.withColumn("row_bt_sum_roll", F.sum(F.col("salary")).over(row_between_spec))

df_row_bt.show()

range_between_spec = Window.partitionBy(F.col("department")) \
    .orderBy(F.col("salary").asc()) \
    .rangeBetween(-100, Window.currentRow)

df_range_bt = df \
    .withColumn("row_bt_emplist", F.collect_list(F.col("employee_name")).over(range_between_spec))

df_range_bt.show()

# check for sorting
df_range_bt.sort(F.col("salary").desc()).show()


spark.stop()