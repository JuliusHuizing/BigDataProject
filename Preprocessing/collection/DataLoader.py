import pandas as pd
import os
from enum import Enum
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

# os.environ['PYSPARK_SUBMIT_ARGS'] = "--master mymaster --total-executor 2 --conf 'spark.driver.extraJavaOptions=-Dhttp.proxyHost=proxy.mycorp.com-Dhttp.proxyPort=1234' -Dhttp.nonProxyHosts=localhost|.mycorp.com|127.0.0.1 -Dhttps.proxyHost=proxy.mycorp.com -Dhttps.proxyPort=1234 -Dhttps.nonProxyHosts=localhost|.mycorp.com|127.0.0.1 pyspark-shell"
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnan, when, count
from pyspark.ml.feature import Imputer, StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import os

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




class DataLoader:
    def __init__(self, split: DataSplit):
        # Create a SparkSession
        self.split = split
        self.spark = SparkSession.builder \
            .appName("ProductReviews") \
            .getOrCreate()
        self._create_schema()
        # self.data = self.load_data()
        # self.df = self._load_data(split)
        # self._load_data()
        # self._clean_data()
            
    def _create_schema(self):
        struct_fields = self.split.get_fields()
        self.schema = StructType(
            struct_fields
        )
        
    def collect_data(self) -> DataFrame:
        # List of file paths for training data
        file_names = self.split.get_file_paths()

        # Read each CSV file into a PySpark DataFrame
        dfs = [self.spark.read.csv(file, header=True, schema=self.schema) for file in file_names]

        # Merge all DataFrames into one
        merged_df = reduce(DataFrame.unionByName, dfs)
        self.df = merged_df
        return self.df
        
  
            
    
        
        
    



