from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class CustomUser:
    def __init__(self, name, age):
        self.name = name
        self.age = age  

def main():
    # Configure Spark Session with Kryo Serializer
    spark = SparkSession.builder \
        .appName("Kryo Serializer Demo") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "512m") \
        .getOrCreate()

    # Register Custom Class with Kryo Serializer
    spark.sparkContext.setCheckpointDir("/tmp/checkpoint")
    spark.sparkContext._jsc.sc().getConf().set(
        "spark.kryo.classesToRegister", "com.example.CustomUser"
    )

    users_data = [
        CustomUser("Rahul", 30),
        CustomUser("Tina", 25),
        CustomUser("Anjali", 26)
    ]

    user_rdd = spark.sparkContext.parallellize(users_data)

    print(f"Number of users : {user_rdd.count()}")

    processed_rdd = users_rdd.map(lambda user: f"{user.name} is {user.age} years old")

    results = processed_rdd.collect()

    for result in results:
        print(result)

    schema = StructType([
        StructField("name", StringType(), False),
        StructField("age", IntegerType(), False)
    ])

    # Convert RDD to Dataframe with Kryo serialized data 

    users_df = spark.createDataFrame(
        data = user_rdd.map(lambda u: (u.name, u.age)),
        schema = schema
    )

    users_df.show()

if __name__ == "__main__":
    main()