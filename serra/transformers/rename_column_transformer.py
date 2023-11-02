from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer

class RenameColumnTransformer(Transformer):
    """
    A transformer to rename a column in a DataFrame.

    :param old_name: The name of the column to be renamed.
    :param new_name: The new name to be assigned to the column.
    """

    def __init__(self, old_name, new_name):
        self.old_name = old_name
        self.new_name = new_name

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

    