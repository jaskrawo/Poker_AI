from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import tomli
import json

def toml_to_json(toml_str):
    try:
        data = tomli.loads(toml_str)
        return json.dumps(data)
    except Exception as e:
        return json.dumps({"error": str(e)})



spark = SparkSession.builder \
    .appName("PokerHandParser") \
    .master("local[*]") \
    .getOrCreate()

toml_to_json_udf = F.udf(toml_to_json, StringType())

# Read TOML files as whole text
raw_df = spark.read.text("pluribus/30/0.phh", wholetext=True).withColumn("filename", F.input_file_name())

print("Raw Data:")
raw_df.show(5, truncate=False)


# Process TOML files
parsed_df = raw_df.withColumn("json_data", toml_to_json_udf(F.col("value"))).withColumn("parsed", F.from_json(F.col("json_data"), MapType(StringType(), StringType())))

print("Parsed Data:")
parsed_df.show(5, truncate=False)

print("Raw TOML to JSON Conversion:")
parsed_df.select("filename", "json_data").show(truncate=False, n=1)

parsed_df.write.mode("overwrite").json("output_json")
