from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer

class DropColumnTransformer(Transformer):
    """
    A transformer to drop specified columns from the DataFrame.

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following key:
                   - 'drop_names': A list of column names to be dropped from the DataFrame.
    """


    def __init__(self, config):
        self.config = config
        self.drop_names = config.get("drop_names")

    def transform(self, df):
        """
        Drop specified columns from the DataFrame.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the specified columns dropped.
        """
        return df.select([c for c in df.columns if c not in self.drop_names])
    