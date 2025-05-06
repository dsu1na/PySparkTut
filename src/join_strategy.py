import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F  


logger = logging.getLogger(__name__)
logging.basicConfig(level = logging.INFO)

spark = SparkSession \
    .builder \
    .appName("Join strategy application") \
    .master("local[*]") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

emp_df = spark.createDataFrame(data=emp, schema=empColumns)

emp_df.printSchema()

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]

deptColumns = ["dept_name","dept_id"]

dept_df = spark.createDataFrame(data=dept, schema=deptColumns)

dept_df.printSchema()

## Carry out join with no hints
emp_df.alias("a") \
    .join(dept_df.alias("b"), on=F.col("a.emp_dept_id")==F.col("b.dept_id"), how="inner") \
    .write.format("noop").mode("overwrite").save()

"""
it will default to sort merge join as it is the preferred join strategy
"""

## Carry out join with broadcast hint
emp_df.hint("broadcast").alias("a") \
    .join(dept_df.alias("b"), on=F.col("a.emp_dept_id")==F.col("b.dept_id"), how="inner") \
    .write.format("noop").mode("overwrite").save()

""" 
The emp_df is broadcasted to all the nodes in the cluster.
"""

## Carry out join with shuffle hash join hint
emp_df.hint("shuffle_hash").alias("a") \
    .join(dept_df.alias("b"), on=F.col("a.emp_dept_id")==F.col("b.dept_id"), how="inner") \
    .write.format("noop").mode("overwrite").save()

"""
"""


time.sleep(120)

spark.stop()