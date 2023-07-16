# Running a specific job
import sys
from sys import exit
from serra.config_parser import ConfigParser
from serra.utils import import_class, get_path_to_user_configs_folder, write_to_file
from serra.aws import read_json_s3, write_json_s3
from os.path import exists
from loguru import logger
from serra.databricks import upload_wheel_to_bucket, restart_server

# Setup logger
logger.remove()  # Remove the default sink
logger.add(sink=sys.stdout, format="<green>{time}</green> - <level>{level}</level> - <cyan>{message}</cyan>", colorize=True)
logger.add("app.log", format="<green>{time}</green> - <level>{level}</level> - <cyan>{message}</cyan>")

def create_job_yaml(job_name):
    file_path = f"{get_path_to_user_configs_folder()}/{job_name}.yml"

    if exists(file_path):
        print("File already exists. Exiting.")
        exit()
    
    starter_config = f"name: {job_name}\nsteps: []"
    write_to_file(file_path, starter_config)

def convert_name_to_full(class_name):
    if class_name in ["LocalReader", "S3Reader", "DatabricksReader"]:
        return f"serra.readers.{class_name}"
    elif class_name in ["LocalWriter", "S3Writer", "DatabricksWriter"]:
        return f"serra.writers.{class_name}"
    else:
        return f"serra.transformers.{class_name}"

def run_job_with_config_parser(cf: ConfigParser, is_local):
    """
    Current assumptions
    - at least one step
    - first step is a read
    - only one read in job steps
    """
    # logger.info(f"Executing tables")
    # tables = cf.get_tables()
    # job_name = cf.get_job_name
    # json_content = read_json_s3(job_name)
    # json_content['tables'] = tables
    # logger.info(f"Executing tables {json_content['tables']}")

    # logger.info(f"Writing tables")
    # write_json_s3(json_content, job_name)
    
    steps = cf.get_job_steps()

    reader_step = steps[0]
    logger.info(f"Executing {reader_step}")
    reader_class_name = cf.get_class_name_for_step(reader_step)
    reader_config = cf.get_config_for_step(reader_step)

    full_reader_class_name = convert_name_to_full(reader_class_name)
    reader_object = import_class(full_reader_class_name)

    df = reader_object(reader_config).read()

    if is_local:
        df = df.limit(10)
        # logger.info(f"Here is starting dataframe {df.show()}")

    for step in steps[1:-1]:
        logger.info(f"Executing {step}")
        # Get coressponding class
        class_name = cf.get_class_name_for_step(step)
        config = cf.get_config_for_step(step)

        full_class_name = convert_name_to_full(class_name)
        step_object = import_class(full_class_name)
        df = step_object(config).transform(df)

    # Assume final step is a write
    writer_step = steps[-1]
    logger.info(f"Executing {writer_step}")
    writer_class_name = cf.get_class_name_for_step(writer_step)
    writer_config = cf.get_config_for_step(writer_step)

    full_writer_class_name = convert_name_to_full(writer_class_name)
    writer_object = import_class(full_writer_class_name)
    writer_object(writer_config).write(df)

    # Commenting this out because it doesn't look good with large dfs
    df.show()

def run_job_from_job_dir(job_name):
    user_configs_folder = get_path_to_user_configs_folder()
    config_path = f"{user_configs_folder}/{job_name}.yml"
    cf = ConfigParser.from_local_config(config_path)
    run_job_with_config_parser(cf, True)

def run_job_from_aws(job_name):
    try:
        cf = ConfigParser.from_s3_config(f"{job_name}.yml")
        run_job_with_config_parser(cf, False)
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
