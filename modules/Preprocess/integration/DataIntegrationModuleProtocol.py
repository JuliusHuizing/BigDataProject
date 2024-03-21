from typing import Protocol
from pyspark.sql import DataFrame
from ..PreprocessingModuleProtocol import PreprocessingModule

from ...Collect.DataCollectorProtocol import DataCollectorProtocol

class DataIntegrationModuleProtocol(Protocol):
    # collector: DataCollectorProtocol
    secondary_dataframe: DataFrame
    column_primary_source: str
    column_secondary_source: str
    # preprocessing_steps: list[PreprocessingModule]
    join: str
    
    def __init__(self, collect: DataCollectorProtocol, preprocess: list[PreprocessingModule], column_primary_source: str, 
                 column_secondary_source: str, join_type: str):
        self.secondary_dataframe = collect.collect_data()[0]
        for step in preprocess:
            self.secondary_dataframe = step.process(self.secondary_dataframe)
            
        self.column_primary_source = column_primary_source
        self.column_secondary_source = column_secondary_source
        self.join = join_type
        # self.collector = collect
        # self.preprocessing_steps = preprocess
        
    def process(self, data: DataFrame) -> DataFrame:
        ...

