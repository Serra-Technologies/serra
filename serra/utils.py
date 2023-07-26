import pkg_resources
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from serra.config import CONFIG_WORKSPACE_NAME

def write_to_file(filename, content):
    try:
        with open(filename, 'w') as file:
            file.write(content)
        print(f"Successfully wrote to {filename}.")
    except IOError:
        print(f"Error writing to {filename}.")

def import_class(cl):
    d = cl.rfind(".")
    classname = cl[d + 1 : len(cl)]
    m = __import__(cl[0:d], globals(), locals(), [classname])
    return getattr(m, classname)

def get_or_create_spark_session():
    # TODO: Take a look for spark session conf: https://engineeringfordatascience.com/posts/pyspark_unit_testing_with_pytest/

    # These installations are only really necessary when running locallys
    return SparkSession.builder.config("spark.logLevel", "ERROR").getOrCreate()

def get_path_to_user_configs_folder():
    return pkg_resources.resource_filename('serra', CONFIG_WORKSPACE_NAME)

def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        sql_script = file.read()
    return sql_script


