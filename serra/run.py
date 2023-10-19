# Running a specific job
import os
import sys
from datetime import datetime
from loguru import logger

from serra.config_parser import ConfigParser
from serra.utils import get_path_to_user_configs_folder, write_to_file
from serra.databricks import upload_wheel_to_bucket, restart_server
from serra.runners.graph_runner import run_job_with_graph
from serra.exceptions import SerraRunException
from serra.profile import SerraProfile

PACKAGE_PATH = os.path.dirname(os.path.dirname(__file__))

# Setup logger
logger.remove()  # Remove the default sink
logger.add(sink=sys.stdout, format="<green>{time}</green> - <level>{level}</level> - <cyan>{message}</cyan>", colorize=True)

def run_job(job_name, config_location):
    """
    You can either run the job with a file that is found locally, or is uploaded to an s3 bucket
    The s3 bucket is specified through the profiles.yml

    Returns a json representation of the run
    """

    assert config_location in ["local", 'aws']

    cf = None
    serra_profile = None

    if config_location == "local":
        user_configs_folder = get_path_to_user_configs_folder()
        config_path = f"{user_configs_folder}/{job_name}.yml"
        cf = ConfigParser.from_local_config(config_path)

        serra_profile = SerraProfile.from_yaml_path("./profiles.yml")
    elif config_location == "aws":
        config_path = f"{job_name}.yml"
        cf = ConfigParser.from_s3_config(config_path)
        raise NotImplementedError("Must grab profiles.yml from AWS as well")

    monitor = run_job_with_graph(cf, serra_profile)
    return monitor.to_dict()

def run_job_safely(job_name, config_location):
    """Wrapper of run job with exception catcher
    """
    result = None
    try:
        print('#####JOB RUN#####', datetime.now())
        result = run_job(job_name, config_location)
    except SerraRunException as s:
        logger.error(s)
    return result

def update_package():
    # create wheel
    # upload wheel to aws
    # tell databricks to delete all packages
    # restart server
    upload_wheel_to_bucket()
    restart_server()
