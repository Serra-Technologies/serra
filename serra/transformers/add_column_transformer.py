from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer
from serra.exceptions import SerraRunException

class AddColumnTransformer(Transformer):
    """
    A transformer to add a new column to the DataFrame with a specified value.

    :param new_column_name: The name of the new column to be added.
    :param value: The value to be set for the new column.
    :param new_column_type: The data type of the new column. Must be a valid PySpark data type string.
    """

    def __init__(self, new_column_name, value, new_column_type):
        self.new_column_name = new_column_name
        self.value = value
        self.new_column_type = new_column_type
        
    def transform(self, df):
        """
        Add a new column to the DataFrame with the specified name, value, and data type.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the new column added.
        :raises: SerraRunException if the column with the specified name already exists in the DataFrame.
        """
        if self.new_column_name in df.columns:
            raise SerraRunException(f"Column '{self.new_column_name}' already exists in the DataFrame. Choose a different name.")
        
        return df.withColumn(
            self.new_column_name, F.lit(self.value).cast(self.new_column_type)  
        )
    