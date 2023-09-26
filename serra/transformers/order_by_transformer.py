from serra.transformers.transformer import Transformer

class OrderByTransformer(Transformer):
    """
    A transformer to sort the DataFrame based on specified columns in ascending or descending order.

    :param columns: A list of column names to sort the DataFrame by.
    :param ascending: Optional. If True (default), sort in ascending order. If False, sort in descending order.
    """

    def __init__(self, columns, ascending=True):
        self.columns = columns
        self.ascending = ascending

    @classmethod
    def from_config(cls, config):
        columns = config.get("columns")
        ascending = config.get("ascending", True)

        obj = cls(columns, ascending)
        obj.input_block = config.get('input_block')
        return obj

    def transform(self, df):
        """
        Transform the DataFrame by sorting it based on specified columns.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the rows sorted based on the specified columns.
        """

        return df.orderBy(*self.columns, ascending = self.ascending)
