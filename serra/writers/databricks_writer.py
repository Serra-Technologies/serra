from pyspark.sql import DataFrame

from serra.writers import Writer

class DatabricksWriter(Writer):
    """
    A writer to write data from a Spark DataFrame to a Databricks Delta table.

    :param config: A dictionary containing the configuration for the writer.
                   It should have the following keys:
                   - 'database': The name of the Databricks database to write to.
                   - 'table': The name of the table in the Databricks database.
                   - 'format': The file format to use for the Delta table.
                   - 'mode': The write mode to use, such as 'overwrite', 'append', etc.
    """

    def __init__(self, config):
        self.config = config
        self.database = self.config.get('database')
        self.table = self.config.get('table')
        self.format = self.config.get('format')
        self.mode = self.config.get('mode')
        
    def write(self, df: DataFrame):
        """
        Write data from a Spark DataFrame to a Databricks Delta table.

        :param df: The Spark DataFrame to be written to the Delta table.
        """
        # Currently forces overwrite if csv already exists
        df.write.format(self.format).mode(self.mode).saveAsTable(f'{self.database}.{self.table}')
        return None

