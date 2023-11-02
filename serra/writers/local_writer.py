from serra.writers.writer import Writer

class LocalWriter(Writer):
    """
    A writer to write data from a Spark DataFrame to a local file.

    :param file_path: The path of the local file to write to.
    """

    def __init__(self, file_path):
        self.file_path = file_path

    def write(self, df):
        """
        Write data from a Spark DataFrame to a local file.

        :param df: The Spark DataFrame to be written to the local file.
        """
        # Convert PySpark DataFrame to Pandas DataFrame
        pandas_df = df.toPandas()

        # Write the Pandas DataFrame to a local file
        pandas_df.to_csv(self.file_path, index=False)