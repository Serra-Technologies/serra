from pyspark.sql import SparkSession
from loguru import logger

def add_big_query_config(spark_builder):
    return spark_builder.config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2")

def misc_config(spark_builder):
    return spark_builder.config("spark.sql.debug.maxToStringFields", 100)
    

def get_or_create_spark_session():
    # TODO: Take a look for spark session conf: https://engineeringfordatascience.com/posts/pyspark_unit_testing_with_pytest/
    builder = SparkSession.builder
    builder = add_big_query_config(builder)
    builder = misc_config(builder)
    return builder.getOrCreate()
