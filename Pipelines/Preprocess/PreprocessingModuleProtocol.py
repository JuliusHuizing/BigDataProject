from typing import Protocol
from pyspark.sql import DataFrame

class PreprocessingModule(Protocol):
    def process(self, data: DataFrame) -> DataFrame:
        pass
        

