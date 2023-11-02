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

    def __init__(self, database, table, format, mode):
        self.database = database
        self.table = table
        self.format = format
        self.mode = mode
        
    def write(self, df: DataFrame):
        """
        Write data from a Spark DataFrame to a Databricks Delta table.

        :param df: The Spark DataFrame to be written to the Delta table.
        """
        # Currently forces overwrite if csv already exists
        df.write.format(self.format).mode(self.mode).saveAsTable(f'{self.database}.{self.table}')
        return None

