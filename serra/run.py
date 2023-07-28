# Running a specific job
import sys
from sys import exit
import os
from serra.config_parser import ConfigParser
from serra.utils import get_path_to_user_configs_folder, write_to_file
from os.path import exists
from loguru import logger
from serra.databricks import upload_wheel_to_bucket, restart_server
from serra.runners.graph_runner import run_job_with_graph
from serra.exceptions import SerraRunException
from serra.translate_client import save_as_yaml, get_translated_yaml

PACKAGE_PATH = os.path.dirname(os.path.dirname(__file__))

# Setup logger
logger.remove()  # Remove the default sink
logger.add(sink=sys.stdout, format="<green>{time}</green> - <level>{level}</level> - <cyan>{message}</cyan>", colorize=True)

def create_job_yaml(job_name):
    file_path = f"{get_path_to_user_configs_folder()}/{job_name}.yml"

    if exists(file_path):
        print("File already exists. Exiting.")
        exit()
    
    starter_config = f"name: {job_name}\nsteps: []"
    write_to_file(file_path, starter_config)

def run_job_from_job_dir(job_name):
    try:
        user_configs_folder = get_path_to_user_configs_folder()
        config_path = f"{user_configs_folder}/{job_name}.yml"
        cf = ConfigParser.from_local_config(config_path)
        run_job_with_graph(cf)
    except SerraRunException as e:
        logger.error(e)

# translates your given sql file, gives you the config output, and saves the config in a new yml file

# Use the paths as needed


def translate_job(sql_path, is_run):
    logger.info(f"Starting translation process for {sql_path}...")
    yaml_path = os.path.splitext(sql_path)[0]

    # Translate job by getting root dir, sql folder, sql path, get response, package path is just serra/
    sql_folder_path = './sql'

    sql_path = f"{sql_folder_path}/{sql_path}"

    translated_yaml = get_translated_yaml(sql_path)
    if not translated_yaml:
        logger.error("Unable to translate file")
        return

    # Save in new yaml file (config folder with same name as sql path)
    user_configs_folder = get_path_to_user_configs_folder()
    yaml_path = f"{user_configs_folder}/{yaml_path}.yml"
    logger.info(f"Translation complete. Yaml file can be found at {os.path.abspath(yaml_path)}")
    save_as_yaml(translated_yaml, yaml_path)

    if is_run:
        logger.info("Running job...")
        cf = ConfigParser.from_local_config(yaml_path)
    # run_job_simple_linear(cf, True)
        try:
            run_job_with_graph(cf)
            logger.info("Job run completed.")
        except SerraRunException as e:
            logger.error(e)

def run_job_from_aws(job_name):
    try:
        cf = ConfigParser.from_s3_config(f"{job_name}.yml")
        run_job_with_graph(cf)
    except SerraRunException as e:
        logger.error(e)
        exit(1)

def update_package():
    # create wheel
    # upload wheel to aws
    # tell databricks to delete all packages
    # restart server
    upload_wheel_to_bucket()
    restart_server()
