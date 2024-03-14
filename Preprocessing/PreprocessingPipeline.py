from PreprocessingProtocol import PreprocessingProtocol
from pyspark.sql import DataFrame

class PreprocessingPipeline:
    def __init__(self, modules: list[PreprocessingProtocol]):
        self.modules = modules

    def augment_data(self, data: DataFrame) -> DataFrame:
        for module in self.modules:
            data = module.process(data)
        return data