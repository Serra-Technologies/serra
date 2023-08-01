from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer

class FilterTransformer(Transformer):
    """
    Test transformer to add a column to dataframe
    :param config: Holds column value
    """

    def __init__(self, config):
        self.config = config
        self.condition = config.get("condition")
        self.column = config.get("column")

    def transform(self, df):
        """
        Add column with col_value to dataframe
        :return; Dataframe w/ new column containing col_value
        """

        return df.filter(df[self.column].isin(self.condition))
    