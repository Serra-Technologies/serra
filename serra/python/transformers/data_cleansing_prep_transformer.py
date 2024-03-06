from serra.python.base import PythonTransformer
import string
import re

class DataCleansingTransformer(PythonTransformer):
    """
    Clean whitespaces, cases, punctuation.

    """

    def __init__(self, columns, case, punctuation=None, whitespace=None):
        self.columns = columns
        self.case = case
        self.punc = punctuation
        self.white_space = whitespace
        
    def transform(self, df):
        """
        Get mapping dictionaries. Keys are whatever use case you want, Values are dictionaries with keys as column name and values as the functions that do that specific use case transform.
        Loop through key-value pairs for the specified dictionaries, assign df[key] = value.

        """

        # Using dictionaries so we can extend later on ie) adding more punctuation cases/white space cases
        # Mappings — Key (type of case), Value (dictionary that has pairs where key is column name, value is function that outputs transformed column)
        case_map = {
            "Upper Case": {col: df[col].str.upper() for col in self.columns},
            "Lower Case": {col: df[col].str.lower() for col in self.columns}
        }
        
        # Iterate through the specified key to get dictionary and iterate through key value pairs to apply the transforms
        for col, cased_col in case_map.get(self.case, {}).items():
            df[col] = cased_col

        white_space_map = {
            "Leading and Trailing Whitespace": {col: df[col].str.strip() for col in self.columns},
            "All Whitespace": {col: df[col].str.replace(" ", "", regex=False) for col in self.columns},
        }

        for col, white_space_col in white_space_map.get(self.white_space, {}).items():
            df[col] = white_space_col

        punc_map = {
            "Punctuation": {col: df[col].str.replace(f"[{re.escape(string.punctuation)}]", "", regex=True) for col in self.columns},
        }

        for col, punc_col in punc_map.get(self.punc, {}).items():
            df[col] = punc_col

        return df


