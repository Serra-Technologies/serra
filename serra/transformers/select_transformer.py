from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer
from serra.exceptions import SerraRunException

class SelectTransformer(Transformer):
    """
    A transformer to perform a SELECT operation on a DataFrame.

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following key:
                   - 'columns': A list of column names to select from the DataFrame.
    """

    def __init__(self, config):
        self.config = config
        self.columns = config.get("columns", [])

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Perform the SELECT operation on the DataFrame.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame containing only the selected columns.
        :raises: SerraRunException if no columns are specified in the configuration
                 or if none of the specified columns exist in the DataFrame.
        """
        if not self.columns:
            raise SerraRunException("No columns specified in the configuration.")

        selected_columns = [F.col(col) for col in self.columns if col in df.columns]

        if not selected_columns:
            raise SerraRunException("None of the specified columns exist in the DataFrame.")

        return df.select(*selected_columns)



