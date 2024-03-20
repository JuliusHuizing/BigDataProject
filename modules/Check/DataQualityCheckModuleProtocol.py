from typing import Protocol
from pyspark.sql import DataFrame

class DataQualityCheckModule(Protocol):
    
    continue_on_failure: bool
    expectation_suite_path: str
    
    def __init__(self, expectation_suite_path: str, continue_on_failure: bool):
        pass
    
    def _check(self, data: DataFrame) -> bool:
        pass

    def process(self, data: DataFrame) -> DataFrame:
        if not self._check(data) and not self.continue_on_failure:
            raise ValueError("Data quality check failed")
        else:
            return data
    
        

