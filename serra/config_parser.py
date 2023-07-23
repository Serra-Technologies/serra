# In case we ever want to change use of yaml file
import yaml
from serra.aws import retrieve_file_as_bytes_from_bucket


def convert_name_to_full(class_name):
    if "Reader" in class_name:
        return f"serra.readers.{class_name}"
    elif "Writer" in class_name:
        return f"serra.writers.{class_name}"
    else:
        return f"serra.transformers.{class_name}"

class ConfigParser:

    def __init__(self, config):
        self.config = config

    @staticmethod
    def from_local_config(config_path: str):
        with open(config_path, 'r') as stream:
            config = yaml.safe_load(stream)
        return ConfigParser(config)
    
    @staticmethod
    def from_s3_config(config_name):
        # TODO: Make more generalizable
        # Currenltly 
        config_bytes = retrieve_file_as_bytes_from_bucket(config_name)
        config = yaml.safe_load(config_bytes)
        return ConfigParser(config)
    
    def get_step(self, step_name):
        return self.config.get(step_name)
    
    def get_class_name_for_step(self, step_name):
        step = self.get_step(step_name)
        return step.get("class_name")
    
    def get_config_for_step(self, step_name):
        step = self.get_step(step_name)
        return step.get("config")
    
    def get_job_name(self):
        return self.get("name")
    
    def get_blocks(self):
        blocks = [name for name in self.config.keys() if name != 'debug']
        return blocks
    
    def get_config_for_block(self, block_name):
        return self.config.get(block_name).get("config")
    
    def get_tests_for_block(self, block_name):
        return self.config.get(block_name).get("tests")
    
    def get_test(self):
        return self.config.get('debug')

if __name__=="__main__":
    cp = ConfigParser("")