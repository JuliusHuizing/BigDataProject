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


class DataCleaner:
    @staticmethod
    def clean_data(data: DataSet) -> DataSet:
        if data.split != DataSplit.TRAIN:
            logging.warning("Potentially dropping data from non-training set. This is not recommended.")
        data = DataCleaner.remove_duplicate_rows(data)
        data = DataCleaner.drop_dirty_data(data)
        return data
        
    @staticmethod
    def remove_duplicate_rows(data: DataSet) -> DataSet:
        # Delete duplicate rows
        duplicate_rows = data.df.count() - data.df.dropDuplicates().count()
        print(f"Removed {duplicate_rows} duplicate rows.")
        return data
       
    @staticmethod 
    def drop_dirty_data(data: DataSet) -> DataSet:        
        # Ensuring categorical values can only have correct values
        data.df = data.df.withColumn("vine", when(data.df["vine"].isin('Y', 'N'), data.df["vine"]).otherwise(None))
        data.df = data.df.withColumn("verified_purchase", when(data.df["verified_purchase"].isin('Y', 'N'), data.df["verified_purchase"]).otherwise(None))
        data.df = data.df.withColumn("label", when(data.df["label"].isin('True', 'False'), data.df["label"]).otherwise(None))
        
        # Select columns with categorical values
        columns_to_check = ["vine", "verified_purchase", "label"]
        # Remove rows where specified columns contain null or NaN values
        for column in columns_to_check:
            data.df = data.df.filter(col(column).isNotNull())
            
        return data
            