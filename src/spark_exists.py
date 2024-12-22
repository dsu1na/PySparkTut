from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import time


# create spark session
spark = SparkSession \
    .builder \
    .appName("exists programe") \
    .master("local[*]") \
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

emp_df.createOrReplaceTempView("emp")
dept_df.createOrReplaceTempView("dept")

spark.sql("""
select * from emp
where exists (select 1 from dept where dept.dept_id = emp.emp_dept_id)
""").show()



spark.sql("""
select * from emp
where not exists (select 1 from dept where dept.dept_id = emp.emp_dept_id)
""").show()

spark.stop()