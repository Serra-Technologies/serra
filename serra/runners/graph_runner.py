from serra.config_parser import ConfigParser, convert_name_to_full
from serra.utils import get_or_create_spark_session, import_class
from serra.tests import duplicates_test, nulls_test
from loguru import logger
from serra.runners.ExecutionGraph import BlockGraph


# Returns instatiated reader,writer,transformer class with config already passed in
def get_configured_block_object(block_name, cf: ConfigParser):
    config = cf.get_config_for_block(block_name)
    class_name = cf.get_class_name_for_step(block_name)

    full_class_name = convert_name_to_full(class_name)
    block_class = import_class(full_class_name)
    configured_block_object = block_class(config)
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
    block_names = cf.get_blocks()
    graph = BlockGraph([])
    for block_name in block_names:
        obj = get_configured_block_object(block_name, cf)
        graph.add_block(block_name, obj.dependencies)

    order = []
    entry_points = graph.find_entry_points()
    while len(entry_points) != 0:
        order.append(entry_points[0])
        graph.execute(entry_points[0])
        entry_points = graph.find_entry_points()

    return order

def run_job_with_graph(cf: ConfigParser):
    ordered_block_names = get_order_of_execution(cf)
    logger.info(f"Decided order of execution: {ordered_block_names}")
    df_map = {}

    for block_name in ordered_block_names:
        logger.info(f"Executing step {block_name}")
        # execute block
        block_obj = get_configured_block_object(block_name, cf)

        if is_reader(block_name, cf):
            df = block_obj.read()
            df_map[block_name] = df
        elif is_writer(block_name, cf):
            assert len(block_obj.dependencies) == 1
            dependency = block_obj.dependencies[0]
            df_map[dependency].show()
            block_obj.write(df_map[dependency]) # Get the 
        elif is_Transformer(block_name, cf):
            assert len(block_obj.dependencies) >= 1
            input_dfs = [df_map[dep] for dep in block_obj.dependencies]
            df = block_obj.transform(*input_dfs)
            df_map[block_name] = df

        if cf.get_tests_for_block(block_name) is not None:
            tests = cf.get_tests_for_block(block_name)
            for test_name in tests:
                print(test_name)
                if test_name == 'nulls':
                    df = df_map[block_name]
                    print('checking')
                    nulls_test(df)
                if test_name == 'duplicates':
                    df = df_map[block_name]
                    print('checking')
                    duplicates_test(df)

    if cf.get_test():
        logger.info("Debug is true—printing out rows.")
        df.show(500)
