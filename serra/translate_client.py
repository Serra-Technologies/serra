import os
import requests
import yaml
from loguru import logger
from serra.config import TRANSLATE_URL

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
    url = TRANSLATE_URL
    response = send_post_request(file_path,url)
    if response.status_code != 200:
        return None
    generated_yaml = response.content.decode('utf-8')
    logger.info(generated_yaml)
    return generated_yaml