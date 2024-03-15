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
import logging
 

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
    def __init__(self, data_dir: str, merge: bool):
        # Create a SparkSession
        self.spark = SparkSession.builder \
            .appName("ProductReviews") \
            .getOrCreate()
        # self._create_schema()
        self.merge = merge
        self.data_dir = data_dir
        # self.data = self.load_data()
        # self.df = self._load_data(split)
        # self._load_data()
        # self._clean_data()
            
    # def _create_schema(self):
    #     # struct_fields = self.split.get_fields()
    #     self.schema = StructType(
    #         struct_fields
    #     )
        
    def collect_data(self) -> list[DataFrame]:
        # List of file paths for training data
        files = []
        for file in os.listdir(self.data_dir):
            if file.endswith('.csv'):
                files.append(self.data_dir + file)
            else:
                logging.warning(f"File {file} is not a CSV file and will be ignored.")
        

        # Read each CSV file into a PySpark DataFrame
        TRAIN_SCHEMA = StructType([
        # although ids are not in specified as column in csv, they are there. So ignore the runtime warnings there.
        StructField("id", IntegerType(), True), 
        StructField("product_id", StringType(), True),
        StructField("product_parent", IntegerType(), True),
        StructField("product_title", StringType(), True),
        StructField("vine", StringType(), True),
        StructField("verified_purchase", StringType(), True),
        StructField("review_headline", StringType(), True),
        StructField("review_body", StringType(), True),
        StructField("review_date", StringType(), True),
        StructField("marketplace_id", IntegerType(), True),
        StructField("product_category_id", IntegerType(), True),                
        StructField("label", StringType(), True)
    ])
        self.dfs = [self.spark.read.csv(file, header=True, schema=TRAIN_SCHEMA) for file in files]
        # Merge all DataFrames into one
        if self.merge:
            merged_df = reduce(DataFrame.unionByName, self.dfs)
            self.dfs = [merged_df]
        return self.dfs
        
  
            
    
        
        
    



