
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, expr


from pyspark.sql import SparkSession
import json

# Create a Spark session
spark = SparkSession.builder.appName("ReadJsonFromFile").getOrCreate()

# Path to the plain text file
text_file_path = "eshyft/data/licenses.txt"

# Read and process the text file line by line
json_data = []
with open(text_file_path, "r") as file:
    for line in file:
        json_object = json.loads(line)
        json_data.append(json_object)

# Create a DataFrame from the parsed JSON data
df = spark.createDataFrame(json_data)
test_list = list(df.select('licenses').collect()[0]['licenses'][0].keys())

df = df.select("*", explode(col("licenses"))).drop("licenses")
df = df.withColumnRenamed("col","licenses")

test_list = list(df.select('licenses').collect()[0]['licenses'][0].keys())
select_columns = [col(f"licenses.{col_name}") for col_name in test_list]

result_df = df.select("*", *select_columns).drop('licenses')
result_df.show()