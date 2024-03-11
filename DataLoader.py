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


# enum for train and test data

 
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
        
         
        

class DataLoader:
    def __init__(self, data_set: DataSet):
        # Create a SparkSession
        self.spark = SparkSession.builder \
            .appName("ProductReviews") \
            .getOrCreate()
        self.data_set = data_set
        self._create_schema()
        self._load_data()
        self._clean_data()
            
    def _create_schema(self):
        self.schema = StructType([
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
        
    def _load_data(self):
        # List of file paths for training data
        file_names = self.data_set.get_file_paths()

        # Read each CSV file into a PySpark DataFrame
        dfs = [self.spark.read.csv(file, header=True, schema=self.schema) for file in file_names]

        # Merge all DataFrames into one
        merged_df = reduce(DataFrame.unionByName, dfs)
        self.df = merged_df
        
    def _clean_data(self):
        self._remove_duplicate_rows()
        self._drop_dirty_data()
        
    def _remove_duplicate_rows(self):
        # Delete duplicate rows
        duplicate_rows = self.df.count() - self.df.dropDuplicates().count()
        self.df.dropDuplicates()
        print(f"Removed {duplicate_rows} duplicate rows.")
        
    def _drop_dirty_data(self):
        # Ensuring categorical values can only have correct values
        self.df = self.df.withColumn("vine", when(self.df["vine"].isin('Y', 'N'), self.df["vine"]).otherwise(None))
        self.df = self.df.withColumn("verified_purchase", when(self.df["verified_purchase"].isin('Y', 'N'), self.df["verified_purchase"]).otherwise(None))
        self.df = self.df.withColumn("label", when(self.df["label"].isin('True', 'False'), self.df["label"]).otherwise(None))
        
        # Select columns with categorical values
        columns_to_check = ["vine", "verified_purchase", "label"]

        # Remove rows where specified columns contain null or NaN values
        self.df = self.df.filter(
            (col(columns_to_check[0]).isNotNull()) &
            (col(columns_to_check[1]).isNotNull()) &
            (col(columns_to_check[2]).isNotNull())
        )
        
        
    


    