import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from serra.python.base import PythonTransformer

class TokenizeTransformer(PythonTransformer):
    """
    Tokenize your strings.
    """

    def __init__(self, columns):
        self.columns = columns
        
    def transform(self, df):
        """
        Get mapping dictionaries. Keys are whatever use case you want, Values are dictionaries with keys as column name and values as the functions that do that specific use case transform.
        Loop through key-value pairs for the specified dictionaries, assign df[key] = value.

        """

        stop_words = set(stopwords.words('english'))
        df[['tokenized_' + col for col in self.columns]] = df[self.columns].applymap(lambda x: [word for word in word_tokenize(x) if word not in stop_words])

        return df


