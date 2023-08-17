from google.cloud import bigquery

from serra.config import BIGQUERY_ACCOUNT_INFO_PATH
from serra.exceptions import SerraRunException

class BigQueryWriter():
    """
    A reader to write data to BigQuery from a Spark DataFrame.

    :param config: A dictionary containing the configuration for the reader.
                   It should have the following keys:
                   - 'project_id'
                   - 'dataset_id'
                   - 'table_id'
    """

    def __init__(self, config):
        self.config = config
    
    @property
    def project_id(self):
        return self.config.get("project_id")
    
    @property
    def dataset_id(self):
        return self.config.get("dataset_id")
    
    @property
    def table_id(self):
        return self.config.get("table_id")
    
    @property
    def mode(self):
        mode = self.config.get("mode")
        valid_modes = ['append', 'overwrite', 'error', 'ignore']
        if mode not in valid_modes:
            raise SerraRunException(f"Invalid BigQueryWriter mode: {mode}, should be one of {valid_modes}")
        return self.config.get("mode")

    @property
    def dependencies(self):
        return [self.config.get('input_block')]
    
    def write(self, df):
        """
        Read data from Snowflake and return a Spark DataFrame.

        :return: A Spark DataFrame containing the data read from the specified Snowflake table.
        """
        df.write \
            .format("bigquery") \
            .option('project', self.project_id)\
            .option("writeMethod", "direct") \
            .mode(self.mode)\
            .save(f"{self.dataset_id}.{self.table_id}")