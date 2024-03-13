import pandas as pd
import os
from enum import Enum
from DataSplit import DataSplit, DataSet

import logging
# os.environ['PYSPARK_SUBMIT_ARGS'] = "--master mymaster --total-executor 2 --conf 'spark.driver.extraJavaOptions=-Dhttp.proxyHost=proxy.mycorp.com-Dhttp.proxyPort=1234' -Dhttp.nonProxyHosts=localhost|.mycorp.com|127.0.0.1 -Dhttps.proxyHost=proxy.mycorp.com -Dhttps.proxyPort=1234 -Dhttps.nonProxyHosts=localhost|.mycorp.com|127.0.0.1 pyspark-shell"
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnan, when, count
from pyspark.ml.feature import Imputer, StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import os
from langdetect import detect
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType



class DataAugmenter:
    # def __init__(self, dataset: DataSet):
    #     self.data = dataset
    #     self.data = self.augment_data()
      
    @staticmethod  
    def augment_data(data: DataSet) -> DataSet:
        data = DataAugmenter.augment_with_review_language(data)
        return data

    @staticmethod 
    def augment_with_review_language(data: DataSet) -> DataSet:
        def detect_language(text: str):
            try:
                detected_language = detect(text)
                return detected_language
            except Exception as e:  
                # print("An error occurred:", e)
                return "unkown language"
            
        detectLanguageUDF = udf(lambda x: detect_language (x),StringType())
        data.df = data.df.withColumn("review_language", detectLanguageUDF(col("review_body")))
        return data

        

        
            
            

            