from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer

class AggregateTransformer(Transformer):
    """
    A transformer to aggregate data based on specified columns and aggregation type.

    :param group_by_columns: A list of column names to group the DataFrame by.
    :param aggregation_type: The type of aggregation to be performed. Supported values: 'sum', 'avg', 'count'.
    """

    def __init__(self, group_by_columns, aggregation_type):
        self.group_by_columns = group_by_columns
        self.aggregation_type = aggregation_type

    def transform(self, df):
        """
        Transform the DataFrame by aggregating data based on the specified configuration.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the aggregated data.
        """
        df = df.groupBy(*self.group_by_columns)

        if self.aggregation_type == 'sum':
            df = df.sum()
        if self.type == 'avg':
            df = df.mean()
        if self.type == 'count':
            df = df.count()

        return df
