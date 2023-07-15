from serra.readers import Reader
from serra.utils import get_or_create_spark_session

class DatabricksReader(Reader):
    def __init__(self, config):
        self.spark = get_or_create_spark_session()
        self.config = config
        self.database = self.config.get('database')
        self.table = self.config.get('table')
        
    def read(self):
        df = self.spark.read.table(f'{self.database}.{self.table}')
        return df


