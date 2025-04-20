from pyspark.sql import SparkSession
from pyspark.scheduler import SparkListener

# create a custom SparkListener
class CustomSparkListener(SparkListener):
    def __init__(self):
        super().__init__()
        self.jobs = 0
        self.stages = 0
        self.tasks = 0

    def onJobStart(self, jobStart):
        self.jobs += 1

    def onStageSubmitted(self, stageSubmitted):
        self.stages += 1
    
    def onTaskStart(self, taskStart):
        self.tasks += 1

# initialize spark and add the listener
spark = SparkSession.builder \
    .appName("CustomSparkListenerExample") \
    .config("spark.ui.port", "4050") \
    .getOrCreate()

sc = spark.sparkContext
    
listener = CustomSparkListener()
sc.addSparkListener(listener)


# print the job stats
print(f"jobs: {listener.jobs}, Stages: {listener.stages}, Tasks: {listener.tasks}")
