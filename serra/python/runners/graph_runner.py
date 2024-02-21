from loguru import logger

from serra.config_parser import ConfigParser
from serra.utils import import_class
from serra.runners.ExecutionGraph import BlockGraph
from serra.python.runners.monitor import Monitor
from serra.python.base import PythonStep

def convert_name_to_full_python(class_name):
    if "Reader" in class_name:
        return f"serra.python.readers.{class_name}"
    elif "Writer" in class_name:
        return f"serra.python.writers.{class_name}"
    else:
        return f"serra.python.transformers.{class_name}"

# Returns instatiated reader,writer,transformer class with config already passed in
def get_configured_block_object(block_name, cf: ConfigParser) -> PythonStep:
    config = cf.get_config_for_block(block_name)
    class_name = cf.get_class_name_for_step(block_name)

    full_class_name = convert_name_to_full_python(class_name)
    block_class = import_class(full_class_name)
    configured_block_object = block_class.from_config(config)
    # configured_block_object = block_class(config)
    return configured_block_object

# TODO: Add execute method to all blocks to remove this design
def is_reader(block_name, cf: ConfigParser):
    class_name = cf.get_class_name_for_step(block_name)
    return "Reader" in class_name

def is_writer(block_name, cf: ConfigParser):
    class_name = cf.get_class_name_for_step(block_name)
    return "Writer" in class_name

def is_Transformer(block_name, cf: ConfigParser):
    class_name = cf.get_class_name_for_step(block_name)
    return "Transformer" in class_name

def get_order_of_execution(cf: ConfigParser):
    # Get all the blocks and initialize the objects
    block_names = cf.get_blocks()
    graph = BlockGraph([])
    for block_name in block_names:
        # obj = get_configured_block_object(block_name, cf)
        dependencies = cf.get_dependencies_for_block(block_name)
        graph.add_block(block_name, dependencies)

    # Now determine a possible order of execution
    order = []
    entry_points = graph.find_entry_points()
    while len(entry_points) != 0:
        order.append(entry_points[0])
        graph.execute(entry_points[0])
        entry_points = graph.find_entry_points()

    return order

def run_job_with_graph(cf: ConfigParser) -> Monitor:
    """
    This function allows us to run a job that contains blocks connected through the
    input_block parameter in each of the transformers and writers. The basic idea
    is to construct a graph of all the blocks connected to their dependencies. We then
    choose to "execute" the node on the graph that has no dependencies. Once it is executed,
    we prune that block from the graph. We continue until execution is complete.

    The df_map dict is used to store the intermediate dataframes that are created during the 
    course of this execution. For example, if a block called step_read produces a dataframe df1,
    that dataframe can be later accessed through df_map['step_read']. This allows blocks that will
    be executed later to access this dataframe and link together properly.

    TODO: Verify that all nodes are connected in graph
    TODO: Verify there are no circular dependencies
    """

    monitor = Monitor()

    ordered_block_names = get_order_of_execution(cf)
    logger.info(f"Decided order of execution: {ordered_block_names}")
    df_map = {}

    for block_name in ordered_block_names:
        logger.info(f"Executing step {block_name}")

        block_obj = get_configured_block_object(block_name, cf)

        if is_reader(block_name, cf):
            df = block_obj.read()

            # Set the dataframe reference in the map
            df_map[block_name] = df
        elif is_writer(block_name, cf):
            assert len(block_obj.dependencies) == 1
            
            input_block_name = block_obj.dependencies[0]
            input_df = df_map[input_block_name]
            block_obj.write(input_df)
        elif is_Transformer(block_name, cf):
            assert len(block_obj.dependencies) >= 1

            input_dfs = [df_map[dep] for dep in block_obj.dependencies]
            df = block_obj.transform(*input_dfs)

            # Set the dataframe reference in the map
            df_map[block_name] = df

        monitor.log_job_step(block_name, df)

        print(df)

    # response = monitor.to_dict()
    # for key, value in response.items():
    #     print(key)
    #     print(value)

    return monitor