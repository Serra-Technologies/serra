"""
Pandas implemenaation
"""
from serra.python_v1.base import PythonTransformer

class AddColumnTransformer(PythonTransformer):
    def __init__(self, new_column_name, value, new_column_type):
        self.new_column_name = new_column_name
        self.value = value
        self.new_column_type = new_column_type

    def transform(self, df):
        df[self.new_column_name] = self.value
        df[self.new_column_name] = df[self.new_column_name].astype(self.new_column_type)
        return df