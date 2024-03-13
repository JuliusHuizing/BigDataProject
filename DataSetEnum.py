import pandas as pd
import os
from enum import Enum


# os.environ['PYSPARK_SUBMIT_ARGS'] = "--master mymaster --total-executor 2 --conf 'spark.driver.extraJavaOptions=-Dhttp.proxyHost=proxy.mycorp.com-Dhttp.proxyPort=1234' -Dhttp.nonProxyHosts=localhost|.mycorp.com|127.0.0.1 -Dhttps.proxyHost=proxy.mycorp.com -Dhttps.proxyPort=1234 -Dhttps.nonProxyHosts=localhost|.mycorp.com|127.0.0.1 pyspark-shell"
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnan, when, count
from pyspark.ml.feature import Imputer, StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import os
 
class DataSet(Enum):
    TRAIN = 1
    VALIDATION = 2
    TEST = 3
    
    def get_file_paths(self) -> list:
        base_path = 'data/'
        result = []
        if self == DataSet.TRAIN:
            # append all files that start with 'train'
            for file in os.listdir(base_path):
                if file.startswith('train'):
                    result.append(base_path + file)
        elif self == DataSet.VALIDATION:
            result.append(f'{base_path}validation_hidden.csv')
        elif self == DataSet.TEST:
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
            case DataSet.TRAIN:
                result.append(StructField("label", StringType(), True))
            case DataSet.VALIDATION:
                pass
            case DataSet.TEST:
                pass
            case _:
                pass
        return result
        
         
        
