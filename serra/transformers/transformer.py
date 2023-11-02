from serra.base import Step
from abc import abstractmethod

class Transformer(Step):
    """
    Transformer base class for data transformations
    Enforce transform method
    """
    @abstractmethod
    def transform(self, df):
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