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

def map_one_file(file_content):
    file, content = file_content
    file_name = "_".join(file.split('/')[-2:]).replace("phh", "json")
    content_json = toml_to_json(content)
    return (file_name, content_json)

def save_file(file_content):
    file_name, content = file_content
    with open(f"raw_json_files/" + file_name, "w") as outfile:
        outfile.write(content)

spark = SparkSession.builder \
    .appName("PokerHandParser") \
    .master("local[*]") \
    .getOrCreate()


rddFromFile = spark.sparkContext.wholeTextFiles("pluribus/*/*.phh")
rdd_mapped = rddFromFile.map(map_one_file)
rdd_save = rdd_mapped.map(save_file)
rdd_save.collect()
