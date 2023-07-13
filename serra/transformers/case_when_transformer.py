from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer

class CaseWhenTransformer(Transformer):
    """
    Test transformer to add a column to dataframe
    :param config: Holds column value
    """

    def __init__(self, config):
        self.config = config
        self.name = config.get("name")
        self.value = config.get("value")

    def transform(self, df):
        """
        Add column with col_value to dataframe
        :return; Dataframe w/ new column containing col_value
        """

        return df.withColumn(
            self.name, F.lit(self.value)
        )
    