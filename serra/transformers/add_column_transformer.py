from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer
from serra.exceptions import SerraRunException

class AddColumnTransformer(Transformer):
    """
    Test transformer to add a column to dataframe
    :param config: Holds column value
    """

    def __init__(self, config):
        self.config = config
        self.name = config.get("name")
        self.value = config.get("value")
        self.column_type = config.get("column_type")
        
    def transform(self, df):
        """
        Add column with col_value to dataframe
        :return; Dataframe w/ new column containing col_value
        """
        if self.name in df.columns:
            raise SerraRunException(f"Column '{self.name}' already exists in the DataFrame. Choose a different name.")
        
        return df.withColumn(
            self.name, F.lit(self.value).cast(self.column_type)  
        )
    