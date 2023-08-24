from google.cloud import bigquery
import os
from serra.readers import Reader
from serra.exceptions import SerraRunException

class BigQueryReader(Reader):
    """
    A reader to read data from Snowflake into a Spark DataFrame.

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
    def dependencies(self):
        return []
    
    def read(self):
        """
        Read data from Snowflake and return a Spark DataFrame.

        :return: A Spark DataFrame containing the data read from the specified Snowflake table.
        """
        # Query to fetch data
        query = f"SELECT * FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`"

        # Execute the query
        bigquery_account_json_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if not bigquery_account_json_path:
            raise SerraRunException("Please set environment variable GOOGLE_APPLICATION_CREDENTIALS to path to Google Cloud Service Account")
        client = bigquery.Client.from_service_account_json(bigquery_account_json_path)
        query_job = client.query(query)

        # Fetch the results
        df = query_job.to_dataframe()

        # Change to spark dataframe
        spark_df = self.spark.createDataFrame(df)
        return spark_df