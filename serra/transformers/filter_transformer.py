from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer

class FilterTransformer(Transformer):
    """
    A transformer to filter the DataFrame based on a specified condition.

    :param filter_values: A list of values to filter the DataFrame by.
    :param filter_column: The name of the column to apply the filter on.
    :param is_expression: Whether the filter is an expression (boolean condition).
    """

    def __init__(self, filter_values, filter_column, is_expression=False):
        self.filter_values = filter_values
        self.filter_column = filter_column
        self.is_expression = is_expression
    
    def transform(self, df):
        """
        Filter the DataFrame based on the specified condition.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with rows filtered based on the specified condition.
        """
        if self.is_expression == None:
            self.is_expression = False
        if self.is_expression:
            return df.filter(F.expr(self.filter_values))

        return df.filter(df[self.filter_column].isin(self.filter_values))
