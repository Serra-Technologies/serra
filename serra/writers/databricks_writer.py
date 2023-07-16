from serra.writers import Writer
from serra.utils import get_or_create_spark_session
from pyspark.sql import DataFrame

class DatabricksWriter(Writer):
    def __init__(self, config):
        self.spark = get_or_create_spark_session()
        self.config = config
        self.database = self.config.get('database')
        self.table = self.config.get('table')
        self.format = self.config.get('format')
        self.mode = self.config.get('mode')
        
    def write(self, df: DataFrame):
        # Currently forces overwrite if csv already exists
        df.write.format(self.format).mode(self.mode).saveAsTable(f'{self.database}.{self.table}')
        return None

