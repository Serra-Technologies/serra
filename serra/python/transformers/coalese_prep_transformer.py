from serra.python.base import PythonTransformer

class CoalesceTransformer(PythonTransformer):
    """
    Coalesces values across multiple columns into a single column.
    """

    def __init__(self, target_column, source_columns):
        """
        Initializes the CoalesceTransformer.

        :param target_column: The column to store the coalesced values.
        :param source_columns: A list of columns to coalesce into the target_column.
        """
        self.target_column = target_column
        self.source_columns = source_columns

    def transform(self, df):
        """
        Performs the coalescing operation on the dataframe.

        :param df: The dataframe to transform.
        :return: The transformed dataframe with coalesced values.
        """
        for column in self.source_columns:
            df[self.target_column] = df[self.target_column].combine_first(df[column])
        return df