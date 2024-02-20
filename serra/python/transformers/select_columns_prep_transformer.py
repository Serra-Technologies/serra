from serra.python.transformers import PythonTransformer

class SelectColumnsTransformer(PythonTransformer):
    """
    Select specified columns

    :param columns: The columns to select
    """

    def __init__(self, columns):
        self.columns = columns

    def transform(self, df):
        return df[self.columns]
