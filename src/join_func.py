"""
There are following types of joins in pyspark

Inner Join
Left Join / Left Outer Join
Right Join / Right Outer Join
Full Outer Join
Cross Join
Anti Join / Left Anti Join
Semi Join / Left Semi Join

Syntax

join(self, other, on=None, how=None)

how should contain any one of 
inner, 
cross, 
outer, full, full_outer, 
left, left_outer,
right, right_outer,
left_semi,
left_anti 
"""



import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)
logging.basicConfig(level = logging.INFO)

spark = SparkSession \
    .builder \
    .appName("Join spark application") \
    .master("local[1]") \
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


## Carry out inner join operation
# emp_df \
#     .join(dept_df, on=emp_df.emp_dept_id==dept_df.dept_id, how="inner") \
#     .show()

# emp_df \
#     .join(dept_df, on=emp_df["emp_dept_id"]==dept_df["dept_id"], how="inner") \
#     .show()

emp_df.alias("a") \
    .join(dept_df.alias("b"), on=F.col("a.emp_dept_id")==F.col("b.dept_id"), how="inner") \
    .show()

emp_df.distinct().show()


spark.stop()