from serra.python.transformers.transformer import Transformer

class SelectColumnsTransformer(Transformer):
    """
    Select specified columns

    :param columns: The columns to select
    """

    def __init__(self, columns):
        self.columns = columns

    def transform(self, df):
        return df[self.columns]
