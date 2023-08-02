from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer

class RenameColumnTransformer(Transformer):
    """
    A transformer to rename a column in a DataFrame.

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following keys:
                   - 'old_name': The name of the column to be renamed.
                   - 'new_name': The new name to be assigned to the column.
    """

    def __init__(self, config):
        self.config = config
        self.old_name = config.get("old_name")
        self.new_name = config.get("new_name")

    def transform(self, df):
        """
        Rename a column in the DataFrame.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the specified column renamed.
        """

        df = df.withColumn(
            self.new_name, F.col(self.old_name)
        )
        return df.drop(F.col(self.old_name))

    