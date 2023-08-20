from serra.readers import Reader


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
        df = self.spark.read.format("bigquery")\
            .option('project', self.project_id)\
                .load(f"{self.dataset_id}.{self.table_id}")
        return df