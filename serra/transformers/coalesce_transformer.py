from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer

class CoalesceTransformer(Transformer):
    """
    A transformer to create a new column by coalescing multiple columns.

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following keys:
                   - 'input_cols': A list of column names to coalesce.
                   - 'output_col': The name of the new column to create with the coalesced values.
    """

    def __init__(self, config):
        self.config = config
        self.input_columns = self.config.get('input_columns')
        self.output_column = self.config.get('output_column')

    def transform(self, df):
        """
        Create a new column by coalescing multiple columns.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the coalesced column.
        """

        return df.withColumn(self.output_column, F.coalesce(*self.input_columns))
