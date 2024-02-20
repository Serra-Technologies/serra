from serra.python.base import PythonReader
import pandas as pd

class LocalReader(PythonReader):
    def __init__(self, path: str):
        self.path = path
    
    def read(self):
        df = pd.read_csv(self.path)
        return df