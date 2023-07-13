from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer

class RenameColumnTransformer(Transformer):
    """
    Test transformer to add a column to dataframe
    :param config: Holds column value
    """

    def __init__(self, config):
        self.config = config
        self.old_name = config.get("old_name")
        self.new_name = config.get("new_name")

    def transform(self, df):
        """
        Add column with col_value to dataframe
        :return; Dataframe w/ new column containing col_value
        """

        df = df.withColumn(
            self.new_name, F.col(self.old_name)
        )
        return df.drop(F.col(self.old_name))

    