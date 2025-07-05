from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("PokerHandParser") \
    .master("local[*]") \
    .getOrCreate()

raw_df = spark.read.text("pluribus/30/*.phh") \
    .withColumn("filename", F.input_file_name()) \
    .withColumn("line_num", F.monotonically_increasing_id())

print("Raw Text Data:")
raw_df.show(5, truncate=False)

