from typing import Protocol
from ..Preprocess.PreprocessingModuleProtocol import PreprocessingModule
from pyspark import SparkContext
from pyspark.sql import DataFrame

class PredictPipelineProtocol(Protocol):
    def __init__(self):
        ...
        
    def predict(self, data: DataFrame, model_path: str, overwrite: bool=False) -> None:
        ...