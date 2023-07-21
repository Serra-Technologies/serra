from serra.config_parser import ConfigParser, convert_name_to_full
from serra.utils import get_or_create_spark_session, import_class
from loguru import logger

def run_job_simple_linear(cf: ConfigParser, is_local):
    """
    Current assumptions
    - at least one step
    - first step is a read
    - only one read in job steps
    """
    steps = cf.get_job_steps()

    reader_step = steps[0]
    logger.info(f"Executing {reader_step}")
    reader_class_name = cf.get_class_name_for_step(reader_step)
    reader_config = cf.get_config_for_step(reader_step)

    full_reader_class_name = convert_name_to_full(reader_class_name)
    reader_object = import_class(full_reader_class_name)

    df = reader_object(reader_config).read()

    if is_local:
        df = df.limit(10)

    for step in steps[1:-1]:
        logger.info(f"Executing {step}")
        # Get coressponding class
        class_name = cf.get_class_name_for_step(step)
        config = cf.get_config_for_step(step)

        full_class_name = convert_name_to_full(class_name)
        step_object = import_class(full_class_name)
        df = step_object(config).transform(df)

    # Assume final step is a write
    writer_step = steps[-1]
    logger.info(f"Executing {writer_step}")
    writer_class_name = cf.get_class_name_for_step(writer_step)
    writer_config = cf.get_config_for_step(writer_step)

    full_writer_class_name = convert_name_to_full(writer_class_name)
    writer_object = import_class(full_writer_class_name)
    writer_object(writer_config).write(df)

    # Commenting this out because it doesn't look good with large dfs
    df.show()

def test_run():
    print("succeeded!")

def run_job_with_graph(cf: ConfigParser):
    blocks = cf.get_blocks()

    for block in blocks:
        print()
        config = cf.get_config_for_block(block)
        class_name = cf.get_class_name_for_step(block)

        full_class_name = convert_name_to_full(class_name)
        block_obj = import_class(full_class_name)
        deps = block_obj(config).dependencies
        print(deps)
        print()

    pass