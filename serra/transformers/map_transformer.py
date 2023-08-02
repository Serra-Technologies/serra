from pyspark.sql import functions as F
import json
from serra.transformers.transformer import Transformer
from serra.exceptions import SerraRunException

class MapTransformer(Transformer):
    """
    A transformer to map values in a DataFrame column to new values based on a given mapping dictionary.

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following keys:
                   - 'name': The name of the new column to be added after mapping.
                   - 'map_dict': A dictionary containing the mapping of old values to new values.
                                 If 'map_dict' is not provided, 'map_dict_path' should be specified.
                   - 'map_dict_path': The path to a JSON file containing the mapping dictionary.
                   - 'col_key': The name of the DataFrame column to be used as the key for mapping.
    """

    def __init__(self, config):
        self.config = config
        self.name = config.get("name")
        self.map_dict = config.get("map_dict")
        self.map_dict_path = config.get("map_dict_path")
        self.col_key = config.get('col_key')

    def transform(self, df):
        """
        Map values in the DataFrame column to new values based on the specified mapping.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the new column containing the mapped values.
        :raises: SerraRunException if any required config parameter is missing or if column specified
                 as 'col_key' does not exist in the DataFrame.
        """
        if not self.name or not self.col_key:
            raise SerraRunException("Both 'name' and 'col_key' must be provided in the config.")
        
        if not self.map_dict and not self.map_dict_path:
            raise SerraRunException("Either 'map_dict' or 'map_dict_path' must be provided in the config.")

        if self.col_key not in df.columns:
            raise SerraRunException(f"Column '{self.col_key}' specified as col_key does not exist in the DataFrame.")

        if self.map_dict is None:
            with open(self.map_dict_path) as f:
                self.map_dict = json.load(f)

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
        
        
    

        
    