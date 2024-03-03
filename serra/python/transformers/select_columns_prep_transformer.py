from serra.python.base import PythonTransformer

class SelectColumnsTransformer(PythonTransformer):
    """
    Select specified columns

    :param columns: The columns to select
    """

    def __init__(self, columns):
        self.columns = columns

    @classmethod
    def from_config(cls, config):
        columns = config.get('columns')

        obj = cls(columns)
        return obj

    def transform(self, df):
        return df[self.columns]
