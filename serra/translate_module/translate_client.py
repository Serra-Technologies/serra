import os
import requests
import time
import json
import shutil

import yaml
from loguru import logger

from serra.config import TRANSLATE_URL

def send_post_request(file_path, url, headers):
    with open(file_path, 'r') as file:
        # Send the POST request with the file
        data = file.read()
        response = requests.post(url, data=data, headers=headers)
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

def reset_serra_token():
    home_dir = os.path.expanduser("~")
    serra_dir = os.path.join(home_dir, ".serra")
    shutil.rmtree(serra_dir)
    get_or_prompt_user_for_serra_token()

def get_or_prompt_user_for_serra_token():
    home_dir = os.path.expanduser("~")
    serra_dir = os.path.join(home_dir, ".serra")
    if not os.path.exists(serra_dir):
        os.mkdir(serra_dir)

    data = {}
    credentials_path = os.path.join(serra_dir, "credentials.json")
    if os.path.exists(credentials_path):
        # Create credentials
        with open(credentials_path, 'r') as file:
            data = json.load(file)
    # user already has 
    if 'serra_token' in data:
        return data['serra_token']
    
    # User does not yet have serra_token
    print("Please authenticate with a serra_token")
    print("If you have not already, please get one from https://cloud.serra.io")
    serra_token = None
    while not serra_token:
        serra_token = input("serra_token: ")

    data["serra_token"] = serra_token
    with open(credentials_path, 'w') as file:
        json.dump(data, file, indent=4)

    return serra_token



def get_translated_yaml(file_path):
    url = TRANSLATE_URL
    create_job_url = url + "start_job"
    check_job_status_url = url + "get_job_status"
    get_job_result_url = url + "get_job_result"
    serra_token = get_or_prompt_user_for_serra_token()

    headers = {
        "Authorization": f'Bearer {serra_token}'
    }

    # Create the job
    job_id_response = send_post_request(file_path,create_job_url, headers)
    if job_id_response.status_code != 200:
        logger.error("Starting job failed.")
        logger.error("Verify if serra_token provided is valid: https://docs.serra.io")
        return None
    job_id = job_id_response.content.decode('utf-8')
    logger.info(f"Received job id {job_id}")

    while True:
        time.sleep(5)
        status_response = requests.get(check_job_status_url, params = {"id": job_id}, headers=headers)
        status = status_response.content.decode('utf-8')
        logger.info(f"Translate status: {status}")
        if status == "SUCCESS":
            break
    
    response = requests.get(get_job_result_url, params={"id": job_id}, headers=headers)
    if response.status_code != 200:
        return None
    generated_yaml = response.content.decode('utf-8')
    logger.info("YAML successfully generated")
    return generated_yaml