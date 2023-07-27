from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer
from serra.exceptions import SerraRunException
import json

class MapTransformer(Transformer):
    """
    Test transformer to add a column to dataframe
    :param config: Holds column value
    """

    def __init__(self, config):
        self.config = config
        self.name = config.get("name")
        self.map_dict = config.get("map_dict")
        self.map_dict_path = config.get("map_dict_path")
        self.col_key = config.get('col_key')

    def transform(self, df):
        """
        Add column with col_value to dataframe
        :return; Dataframe w/ new column containing col_value
        """
        if not self.name or not self.col_key:
            raise SerraRunException("Both 'name' and 'col_key' must be provided in the config.")
        
        if not self.map_dict and not self.map_dict_path:
            raise SerraRunException("Either 'map_dict' or 'map_dict_path' must be provided in the config.")

        if self.col_key not in df.columns:
            raise SerraRunException(f"Column '{self.col_key}' specified as col_key does not exist in the DataFrame.")

        try:
            for key, value in self.map_dict.items():
                df = df.withColumn(f'{self.name}_{key}', F.when(F.col(self.col_key) == key, value))

            # Select the first non-null value from the generated columns
            # create list, then unpack *
            df = df.withColumn(self.name, F.coalesce(*[F.col(f'{self.name}_{key}') for key in self.map_dict]))
            df = df.drop(*[f'{self.name}_{key}' for key in self.map_dict])
        except Exception as e:
            raise SerraRunException(f"Error transforming DataFrame: {str(e)}")

        return df
        
        
    

        
    