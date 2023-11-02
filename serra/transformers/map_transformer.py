from pyspark.sql import functions as F
import json
from serra.transformers.transformer import Transformer
from serra.exceptions import SerraRunException

class MapTransformer(Transformer):
    """
    A transformer to map values in a DataFrame column to new values based on a given mapping dictionary.

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following keys:
                   - 'output_column': The name of the new column to be added after mapping.
                   - 'mapping_dictionary': A dictionary containing the mapping of old values to new values.
                                 If 'mapping_dictionary' is not provided, 'mapping_dict_path' should be specified.
                   - 'mapping_dict_path': The path to a JSON file containing the mapping dictionary.
                   - 'input_column': The name of the DataFrame column to be used as the key for mapping.
    """
    def __init__(self, output_column, input_column, mapping_dictionary=None, mapping_dict_path=None):
        self.output_column = output_column
        self.input_column = input_column
        self.mapping_dictionary = mapping_dictionary
        self.mapping_dict_path = mapping_dict_path

    def transform(self, df):
        """
        Map values in the DataFrame column to new values based on the specified mapping.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the new column containing the mapped values.
        :raises: SerraRunException if any required config parameter is missing or if column specified
                 as 'input_column' does not exist in the DataFrame.
        """
        # if not self.output_column or not self.input_column:
        #     raise SerraRunException("Both 'output_column' and 'input_column' must be provided in the config.")
        
        if not self.mapping_dictionary and not self.mapping_dict_path:
            raise SerraRunException("Either 'mapping_dictionary' or 'mapping_dict_path' must be provided in the config.")

        if self.input_column not in df.columns:
            raise SerraRunException(f"Column '{self.input_column}' specified as input_column does not exist in the DataFrame.")

        if self.mapping_dictionary is None:
            with open(self.mapping_dict_path) as f:
                self.mapping_dictionary = json.load(f)

        try:
            for key, value in self.mapping_dictionary.items():
                df = df.withColumn(f'{self.output_column}_{key}', F.when(F.col(self.input_column) == key, value))

            # Select the first non-null value from the generated columns
            # create list, then unpack *
            df = df.withColumn(self.output_column, F.coalesce(*[F.col(f'{self.output_column}_{key}') for key in self.mapping_dictionary]))
            df = df.drop(*[f'{self.output_column}_{key}' for key in self.mapping_dictionary])
        except Exception as e:
            raise SerraRunException(f"Error transforming DataFrame: {str(e)}")

        return df
        
        
    

        
    