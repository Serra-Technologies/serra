from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer

class DropDuplicatesTransformer(Transformer):
    """
    A transformer to drop duplicate rows from the DataFrame based on specified column(s).

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following keys:
                   - 'cols': A list of column names to identify rows for duplicates removal.
    """

    def __init__(self, config):
        self.config = config
        self.columns_to_check = self.config.get('columns_to_check')

    def transform(self, df):
        """
        Drop duplicate rows from the DataFrame based on specified columns.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with duplicate rows removed based on the specified columns.
        """

        return df.dropDuplicates(self.columns_to_check)
