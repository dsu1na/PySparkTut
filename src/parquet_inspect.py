import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import time
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)
logging.basicConfig(level = logging.INFO)

file_metadata = pq.ParquetFile('../data/covid_tracking.parquet').metadata
print(file_metadata)

# this file has only one row group so the file cannot be splitted
