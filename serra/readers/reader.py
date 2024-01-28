from serra.base import Step
from abc import ABC, abstractmethod

class Reader(Step):
    """
    Reader base class for ingesting data
    Enforce read method
    """
    @abstractmethod
    def read(self, fmt, path, predicate=None):
        pass

    @classmethod
    def from_config(cls, config):
        return cls(**config)

    @property
    def dependencies(self):
        return []
    
    def read_with_spark(self, spark):
        self.spark = spark
        return self.read()
