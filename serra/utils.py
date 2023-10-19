import os
import shutil

from os.path import exists
from loguru import logger
from serra.profile import SerraProfile

def get_local_serra_profile():
    serra_profile = SerraProfile.from_yaml_path("./profiles.yml")
    return serra_profile

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

def dict_to_string(input_dict):
    # Initialize an empty list to store key-value pairs as strings
    key_value_strings = []

    for key, value in input_dict.items():
        # Check if the value is a string and wrap it in double quotes
        if key == 'input_block':
            continue

        if isinstance(value, str):
            value_str = f'"{value}"'
        else:
            value_str = value  # Keep other types as they are

        # Remove single quotes from the key and format the key-value pair
        key_str = key.strip("'")  # Remove single quotes if present
        key_value_strings.append(f'{key_str} = {value_str}')

    # Combine the key-value strings into a single string
    result = ", ".join(key_value_strings)

    return result