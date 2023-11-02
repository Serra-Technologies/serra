from pyspark.sql import functions as F

from serra.transformers.transformer import Transformer

class CastColumnsTransformer(Transformer):
    """
    A transformer to cast columns in a DataFrame to specified data types.

    :param columns_to_cast: A dictionary where the keys are the target column names
                            and the values are lists containing the source column name
                            and the target data type to which the source column will be cast.
                            Example: {'target_column': ['source_column', 'target_data_type']}
    """

    def __init__(self, columns_to_cast):
        self.columns_to_cast = columns_to_cast

    def transform(self, df):
        """
        Cast columns in the DataFrame to specified data types.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the specified columns cast to the target data types.
        """
        for target, [source, target_data_type] in self.columns_to_cast.items():
            df = df.withColumn(target, F.col(source).cast(target_data_type))

        return df