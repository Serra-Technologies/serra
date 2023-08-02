from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer
from serra.exceptions import SerraRunException

class AddColumnTransformer(Transformer):
    """
    A transformer to add a new column to the DataFrame with a specified value.

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following keys:
                   - 'name': The name of the new column to be added.
                   - 'value': The value to be set for the new column.
                   - 'column_type': The data type of the new column. Must be a valid PySpark data type string.
    """


    def __init__(self, config):
        self.config = config
        self.name = config.get("name")
        self.value = config.get("value")
        self.column_type = config.get("column_type")
        
    def transform(self, df):
        """
        Add a new column to the DataFrame with the specified name, value, and data type.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the new column added.
        :raises: SerraRunException if the column with the specified name already exists in the DataFrame.
        """
        if self.name in df.columns:
            raise SerraRunException(f"Column '{self.name}' already exists in the DataFrame. Choose a different name.")
        
        return df.withColumn(
            self.name, F.lit(self.value).cast(self.column_type)  
        )
    