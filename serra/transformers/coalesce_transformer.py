from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer

class CoalesceTransformer(Transformer):
    """
    A transformer to create a new column by coalescing multiple columns.

    :param input_columns: A list of column names to coalesce.
    :param output_column: The name of the new column to create with the coalesced values.
    """

    def __init__(self, input_columns, output_column):
        self.input_columns = input_columns
        self.output_column = output_column

    def transform(self, df):
        """
        Create a new column by coalescing multiple columns.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the coalesced column.
        """

        return df.withColumn(self.output_column, F.coalesce(*self.input_columns))
