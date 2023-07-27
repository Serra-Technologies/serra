from pyspark.sql import SparkSession
from os.path import exists
from loguru import logger

def write_to_file(filename, content):
    try:
        with open(filename, 'w') as file:
            file.write(content)
        logger.info(f"Successfully wrote to {filename}.")
    except IOError:
        logger.error(f"Error writing to {filename}.")

def import_class(cl):
    d = cl.rfind(".")
    classname = cl[d + 1 : len(cl)]
    m = __import__(cl[0:d], globals(), locals(), [classname])
    return getattr(m, classname)

def get_or_create_spark_session():
    # TODO: Take a look for spark session conf: https://engineeringfordatascience.com/posts/pyspark_unit_testing_with_pytest/
    return SparkSession.builder.config("spark.logLevel", "ERROR").getOrCreate()

def get_path_to_user_configs_folder():
    return "./jobs"

def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        sql_script = file.read()
    return sql_script

def validate_workspace():
    if not exists("./jobs"):
        logger.error("Please create a jobs directory before proceeding")
        exit()
    if not exists("./profiles.yml"):
        logger.error("Please create a profiles.yml file in the current directory.")
        exit()
    return

