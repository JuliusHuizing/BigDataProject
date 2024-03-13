from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnan, when, count
from pyspark.ml.feature import Imputer, StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import os
 

import pandas as pd
import os
from enum import Enum

from Config import Config

class DataSplit(Enum):
    TRAIN = 1
    VALIDATION = 2
    TEST = 3
    
    def get_file_paths(self) -> list:
        base_path = Config.DATA_PATH
        result = []
        if self == DataSplit.TRAIN:
            # append all files that start with 'train'
            for file in os.listdir(base_path):
                if file.startswith('train'):
                    result.append(base_path + file)
        elif self == DataSplit.VALIDATION:
            result.append(f'{base_path}{Config.VALIDATION_DATASET_NAME}')
        elif self == DataSplit.TEST:
            result.append(f'{base_path}{Config.TEST_DATASET_NAME}')
        else:
            print("invalid data type")
            return []
        
        return result
    
    def get_fields(self) -> list:
        match self:
            case DataSplit.TRAIN:
                return Config.TRAIN_SCHEMA
            case DataSplit.VALIDATION:
                return Config.VALIDATION_SCHEMA
            case DataSplit.TEST:
                return Config.TEST_SCHEMA
            case _:
                pass
        
# wrapper class for the dataset
# acompany dataframe with split to ensure that the data is not used in the wrong context        
class DataSet:
    
    def __init__(self, df: DataFrame, split: DataSplit):
        self.df = df
        self.split = split


