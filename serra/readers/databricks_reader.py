from serra.readers import Reader
from serra.utils import get_or_create_spark_session
from serra.exceptions import SerraRunException

class DatabricksReader(Reader):
    def __init__(self, config):
        self.spark = get_or_create_spark_session()
        self.config = config
        self.database = self.config.get('database')
        self.table = self.config.get('table')
        
    def read(self):
        try:
            df = self.spark.read.table(f'{self.database}.{self.table}')
        except Exception as e:
            raise SerraRunException(e)
        return df


