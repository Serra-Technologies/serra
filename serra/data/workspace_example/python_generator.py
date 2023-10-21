# GOAL: Take config and output python script
import re

from serra.config_parser import ConfigParser
from serra.runners.graph_runner import get_order_of_execution
from serra.utils import dict_to_string


def camel_to_snake(name):
    # Use regular expressions to find capital letters and insert an underscore before them
    snake_name = re.sub(r'([A-Z])', r'_\1', name)
    # Convert to lowercase and remove leading underscore if present
    return snake_name.lstrip('_').lower()

def parse_config(cf):
    # Returns list of each step name, class, and argument
    ordered_block_names = get_order_of_execution(cf)
    ordered_block_classes = []

    for block_name in ordered_block_names:
        block_class = cf.get_class_name_for_step(block_name)
        block_arguments = dict_to_string(cf.get_config_for_block(block_name))

        ordered_block_classes.append((block_name,block_class,block_arguments))

    return ordered_block_classes

def generate_py_script(ordered_block_classes, cf):
    with open('serra_files.py', 'r') as file:
        script = [file.read()]

    script.append("from pyspark.sql import SparkSession")
    script.append("""spark = SparkSession.builder.appName("PyRun").master("local").getOrCreate()""")

    reader_count = 0 
    join_dict = {}
    for block_name, class_name, args in ordered_block_classes:

        if "Reader" in class_name:
            line = f"df{reader_count + 1} = {class_name}({args}).read_with_spark(spark)"

            # join_dict{step_name: dfx}
            join_dict[block_name] = f"df{reader_count + 1}"
            reader_count += 1

        if "Writer" in class_name:
            line = f"{class_name}({args}).write(df1)"
        if "Transformer" in class_name:
            line = f"df1 = {class_name}({args})."
            if "Join" in class_name:
                # Go into join_on = {read_sales: id, read_ratings: id}, match the keys to the join_dict so you get the appropriate df
                join_args = cf.get_config_for_block(block_name)['join_on']
                matching_keys = [key for key in join_args if key in join_dict]
                join_dfs = [str(join_dict[key]) for key in matching_keys]
                join_dfs_string = ','.join(join_dfs)

                line += f"transform({join_dfs_string})"
            else:
                line += "transform(df1)"


        script.append(line)

    script.append("df1.show()")
    python_script = "\n".join(script)
    return python_script

def save_file(file_name, python_script):
    with open(file_name, 'w') as script_file:
        script_file.write(python_script)

    print(f"The Python script has been saved to {file_name}")

def create_and_save_py_script(config_path):
    cf = ConfigParser.from_local_config(config_path)
    ords = parse_config(cf)
    py_script = generate_py_script(ords,cf)
    save_file("py_script.py", py_script)

if __name__ == "__main__":
    create_and_save_py_script("./jobs/DBDemo.yml")
