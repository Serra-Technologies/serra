from loguru import logger

from serra.config_parser import ConfigParser, convert_name_to_full
from serra.utils import import_class
from serra.tests import duplicates_test, nulls_test
from serra.runners.ExecutionGraph import BlockGraph
from serra.runners.monitor import Monitor
from serra.profile import SerraProfile
from serra.base import Step
from serra.spark import get_or_create_spark_session

# Returns instatiated reader,writer,transformer class with config already passed in
def get_configured_block_object(block_name, cf: ConfigParser) -> Step:
    config = cf.get_config_for_block(block_name)
    class_name = cf.get_class_name_for_step(block_name)

    full_class_name = convert_name_to_full(class_name)
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

def run_job_with_graph(cf: ConfigParser, serra_profile: SerraProfile) -> Monitor:
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
    spark = get_or_create_spark_session(serra_profile)

    ordered_block_names = get_order_of_execution(cf)
    logger.info(f"Decided order of execution: {ordered_block_names}")
    df_map = {}

    for block_name in ordered_block_names:
        logger.info(f"Executing step {block_name}")

        block_obj = get_configured_block_object(block_name, cf)
        block_obj.add_serra_profile(serra_profile)
        block_obj.add_spark_session(spark)

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

        if cf.get_tests_for_block(block_name) is not None:
            tests = cf.get_tests_for_block(block_name)
            for test_name in tests:
                logger.info(f"\tRunning test {test_name}")
                if test_name == 'nulls':
                    df = df_map[block_name]
                    nulls_test(df)
                if test_name == 'duplicates':
                    df = df_map[block_name]
                    duplicates_test(df)

    if cf.show_all():
        logger.info("Showing 500 rows...")
        df.show(500)
    else:
        df.show()

    # response = monitor.to_dict()
    # for key, value in response.items():
    #     print(key)
    #     print(value)

    return monitor

"""
Here is an example of an output of the run_job_with_graph with a job that has two blocks: adder, and read_restaurants

{
    "adder": {
        "columns": [
            "id",
            "rating",
            "new_col"
        ],
        "data": [
            {
                "id": "102",
                "new_col": 1,
                "rating": "0.5"
            },
            {
                "id": "104",
                "new_col": 1,
                "rating": "0.9"
            },
            {
                "id": "105",
                "new_col": 1,
                "rating": "1.7"
            },
            {
                "id": "115",
                "new_col": 1,
                "rating": "3.3"
            },
            {
                "id": "132",
                "new_col": 1,
                "rating": "4.8"
            },
            {
                "id": "133",
                "new_col": 1,
                "rating": "3.9"
            },
            {
                "id": "135",
                "new_col": 1,
                "rating": "3.0"
            },
            {
                "id": "136",
                "new_col": 1,
                "rating": "0.4"
            },
            {
                "id": "137",
                "new_col": 1,
                "rating": "2.5"
            },
            {
                "id": "144",
                "new_col": 1,
                "rating": "2.8"
            },
            {
                "id": "145",
                "new_col": 1,
                "rating": "4.0"
            },
            {
                "id": "145",
                "new_col": 1,
                "rating": "3.0"
            },
            {
                "id": "151",
                "new_col": 1,
                "rating": "1.3"
            },
            {
                "id": "164",
                "new_col": 1,
                "rating": "3.8"
            },
            {
                "id": "166",
                "new_col": 1,
                "rating": "3.8"
            },
            {
                "id": "166",
                "new_col": 1,
                "rating": "4.3"
            },
            {
                "id": "173",
                "new_col": 1,
                "rating": "2.3"
            },
            {
                "id": "177",
                "new_col": 1,
                "rating": "1.3"
            },
            {
                "id": "181",
                "new_col": 1,
                "rating": "2.3"
            },
            {
                "id": "182",
                "new_col": 1,
                "rating": "4.7"
            }
        ],
        "schema": [
            {
                "metadata": {},
                "name": "id",
                "nullable": true,
                "type": "string"
            },
            {
                "metadata": {},
                "name": "rating",
                "nullable": true,
                "type": "string"
            },
            {
                "metadata": {},
                "name": "new_col",
                "nullable": true,
                "type": "integer"
            }
        ]
    },
    "read_restaurants": {
        "columns": [
            "id",
            "rating"
        ],
        "data": [
            {
                "id": "102",
                "rating": "0.5"
            },
            {
                "id": "104",
                "rating": "0.9"
            },
            {
                "id": "105",
                "rating": "1.7"
            },
            {
                "id": "115",
                "rating": "3.3"
            },
            {
                "id": "132",
                "rating": "4.8"
            },
            {
                "id": "133",
                "rating": "3.9"
            },
            {
                "id": "135",
                "rating": "3.0"
            },
            {
                "id": "136",
                "rating": "0.4"
            },
            {
                "id": "137",
                "rating": "2.5"
            },
            {
                "id": "144",
                "rating": "2.8"
            },
            {
                "id": "145",
                "rating": "4.0"
            },
            {
                "id": "145",
                "rating": "3.0"
            },
            {
                "id": "151",
                "rating": "1.3"
            },
            {
                "id": "164",
                "rating": "3.8"
            },
            {
                "id": "166",
                "rating": "3.8"
            },
            {
                "id": "166",
                "rating": "4.3"
            },
            {
                "id": "173",
                "rating": "2.3"
            },
            {
                "id": "177",
                "rating": "1.3"
            },
            {
                "id": "181",
                "rating": "2.3"
            },
            {
                "id": "182",
                "rating": "4.7"
            }
        ],
        "schema": [
            {
                "metadata": {},
                "name": "id",
                "nullable": true,
                "type": "string"
            },
            {
                "metadata": {},
                "name": "rating",
                "nullable": true,
                "type": "string"
            }
        ]
    }
}

"""