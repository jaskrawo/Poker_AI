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


spark = SparkSession.builder \
    .appName("PokerHandParser") \
    .master("local[*]") \
    .getOrCreate()



"""rddFromFile = spark.sparkContext.wholeTextFiles("pluribus/*/*.phh")
rdd_mapped = rddFromFile.map(lambda x: map_one_file(x))


for file_name, content_json in rdd_mapped.collect():
    with open(f"test_dir_2/" + file_name, "w") as outfile:
        outfile.write(content_json)"""


df_json = spark.read.json("test_dir_2/30*.json")
df_json.printSchema()
df_json.show()
