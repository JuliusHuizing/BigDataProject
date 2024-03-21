from pyspark.sql import DataFrame
from typing import Protocol
from pyspark.sql import DataFrame
from ..PreprocessingModuleProtocol import PreprocessingModule

from ...Collect.DataCollectorProtocol import DataCollectorProtocol

class DataJoiner:
    def __init__(self, collect: DataCollectorProtocol, preprocess: list[PreprocessingModule], column_primary_source: str, 
                 column_secondary_source: str, join_type: str):
        self.secondary_dataframe = collect.collect_data()[0]
        for step in preprocess:
            self.secondary_dataframe = step.process(self.secondary_dataframe)
            
        self.column_primary_source = column_primary_source
        self.column_secondary_source = column_secondary_source
        self.join = join_type
        
    def process(self, data: DataFrame) -> DataFrame:
        # joins the dataframes
        data = data.join(self.secondary_dataframe, data[self.column_primary_source] == self.secondary_dataframe[self.column_secondary_source], how=self.join)
        return data
    