from serra.readers import Reader
from serra.utils import get_or_create_spark_session
from loguru import logger

class DatabricksReader(Reader):
    def __init__(self, config):
        self.spark = get_or_create_spark_session()
        self.config = config
        self.database = self.config.get('database')
        self.table = self.config.get('table')
        
    def read(self):
        logger.info("\tReading from Databricks")
        df = self.spark.read.table(f'{self.database}.{self.table}')
        return df


