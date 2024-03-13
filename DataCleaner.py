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
    def __init__(self, dataset: DataSet):
        self.data = dataset
        self.data = self.clean_data()
        

    def clean_data(self) -> DataSet:
        if self.data.split != DataSplit.TRAIN:
            logging.warning("Potentially dropping data from non-training set. This is not recommended.")
        self._remove_duplicate_rows()
        self._drop_dirty_data()
        return self.data
        
    def _remove_duplicate_rows(self):
        # Delete duplicate rows
        duplicate_rows = self.data.df.count() - self.data.df.dropDuplicates().count()
        self.data.df.dropDuplicates()
        print(f"Removed {duplicate_rows} duplicate rows.")
        
    def _drop_dirty_data(self):
        # The predictions should always have the same number of records as the original data, so we only drop dirty data from the training set
        
            
        # Ensuring categorical values can only have correct values
        self.data.df = self.data.df.withColumn("vine", when(self.data.df["vine"].isin('Y', 'N'), self.data.df["vine"]).otherwise(None))
        self.data.df = self.data.df.withColumn("verified_purchase", when(self.data.df["verified_purchase"].isin('Y', 'N'), self.data.df["verified_purchase"]).otherwise(None))
        self.data.df = self.data.df.withColumn("label", when(self.data.df["label"].isin('True', 'False'), self.data.df["label"]).otherwise(None))
        
        # Select columns with categorical values
        columns_to_check = ["vine", "verified_purchase", "label"]
        # Remove rows where specified columns contain null or NaN values
        for column in columns_to_check:
            self.df = self.df.filter(col(column).isNotNull())
            