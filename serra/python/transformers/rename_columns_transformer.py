from serra.python.base import PythonTransformer
import pandas as pd

class RenameColumnsTransformer(PythonTransformer):
    """
    Rename specified columns in the dataset to a new column name.

    """
    def __init__(self, columns_to_change, new_column_name):
        self.columns_to_change = columns_to_change
        self.new_column_name = new_column_name
        
    def transform(self, df):
        for column in self.columns_to_change:
            df.rename(columns={column: self.new_column_name}, inplace=True)
        return df
