from serra.readers import Reader
from serra.utils import get_or_create_spark_session
from loguru import logger

class S3Reader(Reader):
    def __init__(self, config):
        self.spark = get_or_create_spark_session()
        self.config = config
        self.path = self.config.get('path')
        self.format = self.config.get('format')
        
    def read(self):
        logger.info("--- Reading from S3")
        try:
            df = self.spark.read.format(self.format).option("header","true").load(self.path)
        except:
            raise Exception(f"Failed on read from S3: {self.path}")

        return df


