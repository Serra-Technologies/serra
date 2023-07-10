from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer

class Join(Transformer):
    """
    Join tables together
    :param join_type: Type of join
    :param df1, df2: dataframes
    :param matching_col: column to join on
    """

    def __init__(self, config):
        self.config = config
        self.join_type = config.get("join_type")
        self.matching_col = config.get("matching_col")


    def transform(self, df1, df2):
        """
        Add column with col_value to dataframe
        :return; Dataframe w/ new column containing col_value
        """

        matching_col = self.matching_col
        return df1.join(df2, df1[matching_col] == df2[matching_col], self.join_type).drop(df2[matching_col])


    