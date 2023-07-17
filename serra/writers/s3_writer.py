from serra.writers import Writer
from serra.utils import get_or_create_spark_session
from pyspark.sql import DataFrame
from loguru import logger

class S3Writer(Writer):
    def __init__(self, config):
        self.spark = get_or_create_spark_session()
        self.config = config
        self.path = self.config.get('path')
        self.format = self.config.get('format')
        
    def write(self, df: DataFrame):
        logger.info("--- Writing to S3")
        # Currently forces overwrite if csv already exists
        try:
            df.write.option("header","true").mode('overwrite').format(self.format).save(self.path)
        except:
            raise Exception(f"Failed on write to S3: {self.path}")
        return None

