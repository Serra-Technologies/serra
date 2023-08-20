from serra.readers import Reader

class LocalReader(Reader):
    """
    A reader to read data from a local file into a Spark DataFrame.

    :param config: A dictionary containing the configuration for the reader.
                   It should have the following key:
                   - 'file_path': The path to the local file to be read.
    """

    def __init__(self, config):
        self.config = config
        self.file_path = config.get("file_path")
        
    def read(self):
        """
        Read data from a local file and return a Spark DataFrame.

        :return: A Spark DataFrame containing the data read from the local file.
        """
        #TODO: check all files supports
        df = self.spark.read.format("csv").option("header",True).load(self.file_path)
        return df


