from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer

class AggregateTransformer(Transformer):
    """
    A transformer to aggregate data based on specified columns and aggregation type.

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following keys:
                   - 'group_by': A list of column names to group the DataFrame by.
                   - 'type': The type of aggregation to be performed. Supported values: 'sum', 'avg', 'count'.
    """

    def __init__(self, config):
        self.config = config
        self.group_by = self.config.get('group_by')
        self.type = self.config.get('type')

    def transform(self, df):
        """
        Transform the DataFrame by aggregating data based on the specified configuration.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the aggregated data.
        """
        df = df.groupBy(*self.group_by)

        if self.type == 'sum':
            df = df.sum()
        if self.type == 'avg':
            df = df.mean()
        if self.type == 'count':
            df = df.count()

        return df
