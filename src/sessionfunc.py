import logging
from pyspark.sql import SparkSession

# set the logger
logger = logging.getLogger(__name__)
logging.basicConfig(level = logging.INFO)

# create a sample spark session
# appName : it is shown in the spark UI
# master if run in local [x] represents the number of partitions
spark = SparkSession \
    .builder \
    .appName("sample reading from different files") \
    .master("local[*]") \
    .getOrCreate()

logger.info(f"The hash of spark is:  {spark}")
logger.info(f"The version is:  {spark.version}")

logger.info(f"List all tables in catalog of {spark}")
# list spark catalog tables
spark.range(1).createOrReplaceTempView("test_view")
print(spark.catalog.listTables())

logger.info("Dropping temp view")
spark.catalog.dropTempView("test_view")
print(spark.catalog.listTables())



# to get the existing sparksession use getOrCreate method 
# without passing appName and master 
spark2 = SparkSession.builder.getOrCreate()
logger.info(f"The hash of spark is:  {spark2}")
logger.info(f"The version is:  {spark2.version}")


# enable hive support
spark3 = SparkSession \
    .builder \
    .appName("hive support") \
    .master("local[*]") \
    .enableHiveSupport() \
    .getOrCreate()


spark.stop()
spark2.stop()
spark3.stop()