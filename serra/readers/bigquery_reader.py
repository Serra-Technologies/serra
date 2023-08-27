from google.cloud import bigquery
import os
from serra.readers import Reader
from serra.exceptions import SerraRunException

class BigQueryReader(Reader):
    """
    A reader to read data from Snowflake into a Spark DataFrame.

    :param config: A dictionary containing the configuration for the reader.
                   It should have the following keys:
                   - 'project'
                   - 'dataset'
                   - 'table'
    """

    def __init__(self, config):
        self.config = config
    
    @property
    def project(self):
        return self.config.get("project")
    
    @property
    def dataset(self):
        return self.config.get("dataset")
    
    @property
    def table(self):
        return self.config.get("table")

    @property
    def dependencies(self):
        return []
    
    def read(self):
        """
        Read data from Snowflake and return a Spark DataFrame.

        :return: A Spark DataFrame containing the data read from the specified Snowflake table.
        """
        # Query to fetch data
        query = f"SELECT * FROM `{self.project}.{self.dataset}.{self.table}`"

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