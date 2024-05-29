import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType


logger = logging.getLogger(__name__)
logging.basicConfig(level = logging.INFO)

# create a spark session
spark = SparkSession \
    .builder \
    .appName("Read from json") \
    .master("local[*]") \
    .getOrCreate()

# read a single line json
df = spark \
    .read \
    .format("json") \
    .load("./data/zipcodes.json")

logger.info("The schema of json read is:")
df.printSchema()

# read a multiline json
df_multiline = spark \
    .read \
    .format("json") \
    .option("multiline", "true") \
    .load("./data/multiline-zipcodes.json")

logger.info("The dataframe of multiline json read is:")
df_multiline.show()

df_multiple_files = spark \
    .read \
    .format("json") \
    .load(["./data/zipcode1.json", "./data/zipcode2.json"])

logger.info("Reading multiple files")
df_multiple_files.show()


# read all the json files from a folder
df_all_json = spark \
    .read \
    .format("json") \
    .load("./data/*.json")

logger.info("Reading all the json files from a folder")
df_all_json.printSchema()
print(f"The number of rows in the dataframe is : {df_all_json.count()}")


schema = StructType([
      StructField("RecordNumber",IntegerType(),True),
      StructField("Zipcode",IntegerType(),True),
      StructField("ZipCodeType",StringType(),True),
      StructField("City",StringType(),True),
      StructField("State",StringType(),True),
      StructField("LocationType",StringType(),True),
      StructField("Lat",DoubleType(),True),
      StructField("Long",DoubleType(),True),
      StructField("Xaxis",IntegerType(),True),
      StructField("Yaxis",DoubleType(),True),
      StructField("Zaxis",DoubleType(),True),
      StructField("WorldRegion",StringType(),True),
      StructField("Country",StringType(),True),
      StructField("LocationText",StringType(),True),
      StructField("Location",StringType(),True),
      StructField("Decommisioned",BooleanType(),True),
      StructField("TaxReturnsFiled",StringType(),True),
      StructField("EstimatedPopulation",IntegerType(),True),
      StructField("TotalWages",IntegerType(),True),
      StructField("Notes",StringType(),True)
  ])

df_with_schema = spark.read.schema(schema) \
        .json("./data/zipcodes.json")
df_with_schema.printSchema()
df_with_schema.show()

spark.stop()