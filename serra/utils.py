from pyspark.sql import SparkSession
from os.path import exists
from loguru import logger
import os
import shutil

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

def copy_folder(source_folder, destination_folder):
    try:
        # Check if the source folder exists
        if not os.path.exists(source_folder):
            logger.info(f"Source folder '{source_folder}' does not exist.")
            return

        # Check if the destination folder exists
        if not os.path.exists(destination_folder):
            os.makedirs(destination_folder)

        # Copy the contents of the source folder to the destination folder
        for item in os.listdir(source_folder):
            source_item = os.path.join(source_folder, item)
            destination_item = os.path.join(destination_folder, item)
            if os.path.isdir(source_item):
                shutil.copytree(source_item, destination_item)
            else:
                shutil.copy2(source_item, destination_item)

        logger.info(f"Folder copied successfully from '{source_folder}' to '{destination_folder}'.")
    except Exception as e:
        logger.error(f"An error occurred while copying the folder: {e}")