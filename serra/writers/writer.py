from abc import abstractmethod
from serra.base import Step

class Writer(Step):
    """
    Writer base class for loading data
    Enforce write method
    """
    @abstractmethod
    def write(self, df):
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