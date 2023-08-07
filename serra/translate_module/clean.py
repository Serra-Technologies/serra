# Helper functions to clean the translated serra config
import os
import yaml
from serra.config_parser import ConfigParser
from serra.runners.graph_runner import get_order_of_execution

def write_dict_to_yaml(data, file_path):
    with open(file_path, 'w') as file:
        yaml.dump(data, file)

def incrementally_add_to_yaml(block, file_path):
    with open(file_path, 'a') as file:
        yaml.dump(block, file)
        file.write("\n")

def clean_yaml_file(file_path):
    cp = ConfigParser.from_local_config(file_path)
    order_of_execution = get_order_of_execution(cp)

    reordered_dict = {k: cp.config[k] for k in order_of_execution}

    # At this point just remove the existing file
    os.remove(file_path)

    for metadata_tag in cp.get_metadata_tags():
        if metadata_tag in cp.config:
            metadata_dict = {metadata_tag: cp.config.get(metadata_tag)}
            incrementally_add_to_yaml(metadata_dict, file_path)

    for step in order_of_execution:
        step_dict = {step: reordered_dict[step]}
        incrementally_add_to_yaml(step_dict, file_path)

if __name__=="__main__":
    clean_yaml_file("/Users/albertstanley/Code/serra/workspace/jobs/easy_demo.yml")