from google.cloud import bigquery

from serra.exceptions import SerraRunException
from serra.writers import Writer

class BigQueryWriter(Writer):
    """
    A writer to write data to BigQuery from a Spark DataFrame.

    :param config: A dictionary containing the configuration for the reader.
                   It should have the following keys:
                   - 'project_id'
                   - 'dataset_id'
                   - 'table_id'
    """

    def __init__(self, project_id, dataset_id, table_id, mode):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.mode = mode
    
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
