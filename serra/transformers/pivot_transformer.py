from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer

class Pivot(Transformer):
    """
    Join tables together
    :param row_level: column used for row levels
    :param column_level: column used for col levels
    :param sum_col: column to summarise (values)
    """

    def __init__(self, config):
        self.config = config
        self.row_level = config.get("row_level")
        self.column_level = config.get("column_level")
        self.sum_col = config.get("sum_col")

    def transform(self, df):
        """
        Add column with col_value to dataframe
        :return; Dataframe w/ new column containing col_value
        """

        df = df.groupBy(self.row_level).pivot(self.column_level).avg(self.sum_col)
        return df


    