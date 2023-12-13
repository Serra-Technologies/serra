from abc import ABC, abstractmethod

class Step(ABC):
    
    @classmethod
    def from_config(cls, config):
        return cls(**config)

    def add_spark_session(self, spark):
        self.spark = spark