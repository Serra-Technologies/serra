# Running a specific job
import sys
from sys import exit
from serra.config_parser import ConfigParser, convert_name_to_full
from serra.utils import import_class, get_path_to_user_configs_folder, write_to_file
from os.path import exists
from loguru import logger
from serra.databricks import upload_wheel_to_bucket, restart_server
from serra.runners.graph_runner import run_job_with_graph
from serra.translate import Translator
import os

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
    # run_job_simple_linear(cf, True)
        run_job_with_graph(cf)
    except Exception as e:
        logger.error(e)

# translates your given sql file, gives you the config output, and saves the config in a new yml file

# Use the paths as needed


def translate_job(sql_path, is_run):
    logger.info(f"Starting translation process for {sql_path}...")
    yaml_path = os.path.splitext(sql_path)[0]

    # Translate job by getting root dir, sql folder, sql path, get response, package path is just serra/
    sql_folder_path = os.path.join(PACKAGE_PATH, 'sql')
    sql_path = f"{sql_folder_path}/{sql_path}"

    tl = Translator(sql_path)
    response = tl.prompt_gpt()

    # Save in new yaml file (config folder with same name as sql path)
    user_configs_folder = get_path_to_user_configs_folder()
    yaml_path = f"{user_configs_folder}/{yaml_path}.yml"
    logger.info(f"Translation complete. Yaml file can be found at {yaml_path}")
    tl.save_as_yaml(response, yaml_path)

    if is_run:
        logger.info("Running job...")
        cf = ConfigParser.from_local_config(yaml_path)
    # run_job_simple_linear(cf, True)
    try:
        run_job_with_graph(cf)
        logger.info("Job run completed.")
    except Exception as e:
        logger.error(e)

def run_job_from_aws(job_name):
    try:
        cf = ConfigParser.from_s3_config(f"{job_name}.yml")
        run_job_with_graph(cf)
    except Exception as e:
        logger.error(e)
        exit(1)

def update_package():
    # create wheel
    # upload wheel to aws
    # tell databricks to delete all packages
    # restart server
    upload_wheel_to_bucket()
    restart_server()

# from serra.frontend.server import start_server
# def visualize_dag(job_name):
#     # read config from local dir
#     user_configs_folder = get_path_to_user_configs_folder()
#     config_path = f"{user_configs_folder}/{job_name}.yml"
#     cf = ConfigParser.from_local_config(config_path)

#     # get job steps
#     job_steps = cf.get_job_steps()

#     # show dag
#     server_location = "http://127.0.0.1:5000"
#     logger.info(f"Visualization available at {server_location}")
#     start_server(job_steps)
