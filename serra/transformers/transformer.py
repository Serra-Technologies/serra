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

    @property
    def dependencies(self):
        return [self.config.get('input_block')]