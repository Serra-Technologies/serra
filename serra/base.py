from abc import ABC, abstractmethod

class Step(ABC):
    
    @abstractmethod
    def from_config(cls, config):
        pass

    def add_spark_session(self, spark):
        self.spark = spark