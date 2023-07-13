from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer

class GetCountTransformer(Transformer):
    """
    Test transformer to add a column to dataframe
    :param config: Holds column value
    """

    def __init__(self, config):
        self.config = config
        self.group_by = config.get("group_by")
        self.count_col = config.get("count_col")

    def transform(self, df):
        """
        Add column with col_value to dataframe
        :return; Dataframe w/ new column containing col_value
        """

        return df.groupBy(*self.group_by).agg(F.count(self.count_col))
    