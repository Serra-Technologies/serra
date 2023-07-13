from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer

class DropColumnTransformer(Transformer):
    """
    Test transformer to add a column to dataframe
    :param config: Holds column value
    """

    def __init__(self, config):
        self.config = config
        self.drop_names = config.get("drop_names")

    def transform(self, df):
        """
        Add column with col_value to dataframe
        :return; Dataframe w/ new column containing col_value
        """

        return df.select([c for c in df.columns if c not in self.drop_names])
    