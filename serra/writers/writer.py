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

    @property
    def dependencies(self):
        return [self.config.get('input_block')]