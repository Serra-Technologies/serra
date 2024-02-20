
import pandas as pd
from abc import ABC, abstractmethod

class PythonStep(ABC):
    pass

class PythonReader(PythonStep):
    """
    Reader base class for ingesting data
    Enforce read method
    """
    @abstractmethod
    def read(self) -> pd.DataFrame:
        pass

    @classmethod
    def from_config(cls, config):
        c = dict(config)
        obj = cls(**c)
        return obj

    @property
    def dependencies(self):
        return []

class PythonTransformer(PythonStep):
    """
    Transformer base class for data transformations
    Enforce transform method
    """
    @abstractmethod
    def transform(self, df: pd.DataFrame):
        pass

    @classmethod
    def from_config(cls, config):
        c = dict(config)
        input_block = c.pop("input_block")

        obj = cls(**c)
        obj.input_block = input_block
        return obj

    @property
    def dependencies(self):
        return [self.input_block]
    
class PythonWriter(PythonStep):
    """
    Writer base class for loading data
    Enforce write method
    """
    @abstractmethod
    def write(self, df: pd.DataFrame):
        pass

    @classmethod
    def from_config(cls, config):
        c = dict(config)
        input_block = c.pop("input_block")

        obj = cls(**c)
        obj.input_block = input_block
        return obj

    @property
    def dependencies(self):
        return [self.input_block]