from serra.python.base import PythonTransformer
import string
import re

class DropDuplicatesTransformer(PythonTransformer):
    """
    Clean whitespaces, cases, punctuation.

    """

    def __init__(self, columns):
        self.columns = columns
        
    def transform(self, df):
        """
        Get mapping dictionaries. Keys are whatever use case you want, Values are dictionaries with keys as column name and values as the functions that do that specific use case transform.
        Loop through key-value pairs for the specified dictionaries, assign df[key] = value.

        """

        return df.drop_duplicates(subset=self.columns)


