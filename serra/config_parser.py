# In case we ever want to change use of yaml file
import yaml
from serra.aws import retrieve_file_from_config_bucket


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
        config_bytes = retrieve_file_from_config_bucket(config_name)
        config = yaml.safe_load(config_bytes)
        return ConfigParser(config)
    
    def get_step(self, block_name):
        return self.config.get(block_name)
    
    def get_class_name_for_step(self, block_name):
        step = self.get_step(block_name)
        keys = [key for key in step.keys()]
        if "tests" in keys:
            keys.remove("tests")
        return keys[0]
    
    def get_config_for_step(self, block_name):
        step = self.get_step(block_name)
        return step.get("config")
    
    def get_blocks(self):
        blocks = [name for name in self.config.keys() if name != 'debug']
        return blocks
    
    def get_config_for_block(self, block_name):
        class_name = self.get_class_name_for_step(block_name)
        return self.config.get(block_name).get(class_name)
    
    def get_tests_for_block(self, block_name):
        return self.config.get(block_name).get("tests")
    
    def get_test(self):
        return self.config.get('debug')

if __name__=="__main__":
    cp = ConfigParser("")