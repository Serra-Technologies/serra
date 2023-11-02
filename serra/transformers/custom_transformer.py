from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer

## helpers to execute custom transform code
import re
import importlib

def get_user_transform(code_string, desired_function_name):
    # Extract and perform imports
    imports = re.findall(r"from ([\w\.]+) import ([\w ,]+)", code_string)
    for imp in imports:
        module_name, components = imp
        module = importlib.import_module(module_name)
        for component in components.split(","):
            component = component.strip()
            if " as " in component:
                actual_name, alias = component.split(" as ")
                globals()[alias] = getattr(module, actual_name)
            else:
                globals()[component] = getattr(module, component)

    # Execute the provided code string
    local_namespace = {}
    exec(code_string, globals(), local_namespace)
    
    # Rename the transform function
    globals()[desired_function_name] = local_namespace["transform"]

class CustomTransformer(Transformer):

    def __init__(self, code_block):
        self.code_block = code_block

    def transform(self, df):
        custom_transform_func_name = "custom_transform_" + str(id(self))
        get_user_transform(self.code_block, custom_transform_func_name)
        output_df = globals()[custom_transform_func_name](df)
        return output_df