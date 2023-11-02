from serra.readers import Reader
from serra.exceptions import SerraRunException

class DatabricksReader(Reader):
    """
    A reader to read data from a Databricks Delta Lake table into a Spark DataFrame.

    :param config: A dictionary containing the configuration for the reader.
                   It should have the following keys:
                   - 'database': The name of the database containing the table.
                   - 'table': The name of the table to be read.
    """

    def __init__(self, database, table):
        self.database = database
        self.table = table

    def read(self):
        """
        Read data from a Databricks Delta Lake table and return a Spark DataFrame.

        :return: A Spark DataFrame containing the data read from the specified table.
        :raises: SerraRunException if an error occurs during the data reading process.
        """
        try:
            df = self.spark.read.table(f'{self.database}.{self.table}')
        except Exception as e:
            raise SerraRunException(e)
        return df

    def read_with_spark(self, spark):
        self.spark = spark
        return self.read()
