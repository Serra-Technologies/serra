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
        WRITE_APPEND = "WRITE_APPEND"
        """If the table already exists, BigQuery appends the data to the table."""

        WRITE_TRUNCATE = "WRITE_TRUNCATE"
        """If the table already exists, BigQuery overwrites the table data."""

        WRITE_EMPTY = "WRITE_EMPTY"
        """If the table already exists and contains data, a 'duplicate' error is
        returned in the job result."""
        mode = self.config.get("mode")
        valid_modes = ['append', 'truncate', 'empty']
        if mode not in valid_modes:
            raise SerraRunException(f"Invalid BigQueryWriter mode: {mode}, should be one of [{valid_modes}]")
        return self.config.get("mode")

    @property
    def dependencies(self):
        return [self.config.get('input_block')]
    
    def write(self, df):
        """
        Read data from Snowflake and return a Spark DataFrame.

        :return: A Spark DataFrame containing the data read from the specified Snowflake table.
        """
        pandas_df = df.toPandas()
        client = bigquery.Client.from_service_account_json(BIGQUERY_ACCOUNT_INFO_PATH)
        # Query to fetch data
        table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"

        # Write pandas DataFrame to BigQuery table
        job_config = bigquery.LoadJobConfig()
        if self.mode == "append":
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        elif self.mode == "truncate":
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        elif self.mode == "empty":
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
        job = client.load_table_from_dataframe(pandas_df, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete