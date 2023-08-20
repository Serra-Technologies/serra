# Module to help connect to databricks
from loguru import logger
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Task, PythonWheelTask
from databricks.sdk.service.compute import Library, State

from serra.aws import upload_file_to_config_bucket
from serra.utils import get_path_to_user_configs_folder
from serra.utils import get_local_serra_profile
from serra.config import (
    WHEEL_FILE_NAME_IN_BUCKET,
    LOCAL_PATH_TO_WHEEL
)

def get_remote_path_to_wheel():
    serra_profile = get_local_serra_profile()
    bucket = serra_profile.aws_config_bucket
    s3_wheel_path = f"s3://{bucket}/{WHEEL_FILE_NAME_IN_BUCKET}"
    return s3_wheel_path

def create_databricks_workspace_client():
    serra_profile = get_local_serra_profile()
    w = WorkspaceClient(
        host  = serra_profile.databricks_host,
        token = serra_profile.databricks_token
    )
    return w

def upload_wheel_to_bucket():
    # TODO: Add code to recreate wheel
    logger.info(f"Uploading wheel from {LOCAL_PATH_TO_WHEEL} to AWS")
    upload_file_to_config_bucket(LOCAL_PATH_TO_WHEEL)

def create_job(config_name):
    """Use like this: submit_job("StripeExample")
    """
    serra_profile = get_local_serra_profile()
    s3_wheel_path = get_remote_path_to_wheel()

    w = create_databricks_workspace_client()

    # Upload the config file to aws
    logger.info("Uploading config to AWS")
    config_path = f"{get_path_to_user_configs_folder()}/{config_name}.yml"
    upload_file_to_config_bucket(config_path)
    
    # Create the job on databricks
    logger.info("Creating databricks job")
    job_name = config_name

    j = w.jobs.create(
        name=job_name,
        tasks = [
            Task(
                description = "Basic new job",
                existing_cluster_id = serra_profile.databricks_cluster_id,
                python_wheel_task=PythonWheelTask(entry_point="serra_databricks",
                                                package_name="serra",
                                                parameters=[config_name]),
                libraries=[Library(whl=s3_wheel_path)],
                task_key="my-task"
            )
    ]
    )
    logger.info(f"View job at {w.config.host}/#job/{j.job_id}")
    return j

def restart_server():
    w = create_databricks_workspace_client()
    serra_profile = get_local_serra_profile()
    s3_wheel_path = get_remote_path_to_wheel()
    cluster_id = serra_profile.databricks_cluster_id

    logger.info("Restarting cluster")
    w.clusters.ensure_cluster_is_running(cluster_id)

    logger.info("Uninstalling packages")
    w.libraries.uninstall(cluster_id=cluster_id,
                          libraries=[Library(whl=s3_wheel_path)])
    
    current_cluster_state = w.clusters.get(cluster_id).state

    if current_cluster_state != State.TERMINATED:
        w.clusters.restart_and_wait(cluster_id)
    
    w.clusters.ensure_cluster_is_running(cluster_id)

    logger.info("Finished restarting")