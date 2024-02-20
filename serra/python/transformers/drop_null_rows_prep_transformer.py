from serra.python.base import PythonTransformer

class DropNullRowsTransformer(PythonTransformer):
    """
    Drop rows that have nulls in specified column(s).

    :param columns: The columns to look for
    """

    def __init__(self, columns):
        self.columns = columns
        
    def transform(self, df):
        """
        Add a new column to the DataFrame with the specified name, value, and data type.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the new column added.
        :raises: SerraRunException if the column with the specified name already exists in the DataFrame.
        """
        
        return df.dropna(subset=self.columns)
