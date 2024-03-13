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

class DataSplit(Enum):
    TRAIN = 1
    VALIDATION = 2
    TEST = 3
    
    def get_file_paths(self) -> list:
        base_path = 'data/'
        result = []
        if self == DataSplit.TRAIN:
            # append all files that start with 'train'
            for file in os.listdir(base_path):
                if file.startswith('train'):
                    result.append(base_path + file)
        elif self == DataSplit.VALIDATION:
            result.append(f'{base_path}validation_hidden.csv')
        elif self == DataSplit.TEST:
            result.append(f'{base_path}test_hidden.csv')
        else:
            print("invalid data type")
            return []
        
        return result
    
    def get_fields(self) -> list:
        result = [
            # although ids are not in specified as column in csv, they are there. So ignore the runtime warnings there.
            StructField("", IntegerType(), True), 
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
        ]
        match self:
            case DataSplit.TRAIN:
                result.append(StructField("label", StringType(), True))
            case DataSplit.VALIDATION:
                pass
            case DataSplit.TEST:
                pass
            case _:
                pass
        return result
        
# wrapper class for the dataset
# acompany dataframe with split to ensure that the data is not used in the wrong context        
class DataSet:
    
    def __init__(self, df: DataFrame, split: DataSplit):
        self.df = df
        self.split = split


