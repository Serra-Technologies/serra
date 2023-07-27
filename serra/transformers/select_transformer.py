from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from serra.transformers.transformer import Transformer
from serra.exceptions import SerraRunException

class SelectTransformer(Transformer):
    """
    Transformer to perform a SELECT operation on a DataFrame.
    :param config: Holds the list of columns to select from the DataFrame.
    """

    def __init__(self, config):
        self.config = config
        self.columns = config.get("columns", [])

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Perform the SELECT operation on the DataFrame.
        :param df: Input DataFrame.
        :return: Transformed DataFrame with only the selected columns.
        """
        if not self.columns:
            raise SerraRunException("No columns specified in the configuration.")

        selected_columns = [F.col(col) for col in self.columns if col in df.columns]

        if not selected_columns:
            raise SerraRunException("None of the specified columns exist in the DataFrame.")

        return df.select(*selected_columns)



