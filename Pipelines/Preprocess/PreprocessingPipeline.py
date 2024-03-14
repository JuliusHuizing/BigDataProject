from .PreprocessingModuleProtocol import PreprocessingModule
from pyspark.sql import DataFrame
from .collection.DataCollectorProtocol import DataCollectorProtocol

class PreprocessingPipeline:
    def __init__(self, 
                 data_collector: DataCollectorProtocol,
                 preprocessing_modules: list[PreprocessingModule]):
        self.data_collector = data_collector
        self.preprocessing_modules = preprocessing_modules
        self.modules = [module for module in preprocessing_modules]

    def run(self) -> DataFrame:
        self.data = self.data_collector.collect_data()
        for module in self.modules:
            self.data = module.process(self.data)
        return self.data