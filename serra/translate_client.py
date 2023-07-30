import os
import requests
import yaml
from loguru import logger
from serra.config import TRANSLATE_URL
import time

def send_post_request(file_path, url):
    with open(file_path, 'r') as file:
        # Send the POST request with the file
        data = file.read()
        response = requests.post(url, data=data)
        return response

def save_as_yaml(content: str, file_path: str) -> None:
        """
        Save the content as a YAML file.

        :param content: The content to be saved as YAML.
        :param file_path: The path to the output YAML file.
        """
        try:
            yaml_content = yaml.safe_load(content)
            with open(file_path, 'w') as file:
                yaml.dump(yaml_content, file)
            logger.info(f"Content saved as YAML file: {os.path.abspath(file_path)}")
        except Exception as e:
            logger.info(f"Error saving content as YAML file: {str(e)}")

def get_translated_yaml(file_path):
    url = "https://serra-translate-59cfd3dacac9.herokuapp.com/"
    create_job_url = url + "start_job"
    check_job_status_url = url + "get_job_status"
    get_job_result_url = url + "get_job_result"

    # Create the job
    job_id_response = send_post_request(file_path,create_job_url)
    if job_id_response.status_code != 200:
        logger.error("Starting job failed")
        return None
    job_id = job_id_response.content.decode('utf-8')
    logger.info(f"Received job id {job_id}")

    while True:
        time.sleep(5)
        status_response = requests.get(check_job_status_url, params = {"id": job_id})
        status = status_response.content.decode('utf-8')
        logger.info(f"Polling translate status: {status}")
        if status == "SUCCESS":
            break
    
    response = requests.get(get_job_result_url, params={"id": job_id})
    if response.status_code != 200:
        return None
    generated_yaml = response.content.decode('utf-8')
    logger.info("YAML successfully generated")
    return generated_yaml