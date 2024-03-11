import pandas as pd
import os

# os.environ['PYSPARK_SUBMIT_ARGS'] = "--master mymaster --total-executor 2 --conf 'spark.driver.extraJavaOptions=-Dhttp.proxyHost=proxy.mycorp.com-Dhttp.proxyPort=1234' -Dhttp.nonProxyHosts=localhost|.mycorp.com|127.0.0.1 -Dhttps.proxyHost=proxy.mycorp.com -Dhttps.proxyPort=1234 -Dhttps.nonProxyHosts=localhost|.mycorp.com|127.0.0.1 pyspark-shell"
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnan, when, count
from pyspark.ml.feature import Imputer, StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import os


class DataLoader:
    def __init__(self):
        # Create a SparkSession
        self.spark = SparkSession.builder \
            .appName("ProductReviews") \
            .getOrCreate()
        self._create_schema()
        self._load_train_data()
        self._clean_train_data()
            
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
        
    def _load_train_data(self):
        # List of file paths for training data
        data_folder = 'data'
        data_files = 8

        train_files = []

        for file in range(1, data_files+1):
            train_files.append(f'{data_folder}/train-{file}.csv')

        # Read each CSV file into a PySpark DataFrame
        dfs = [self.spark.read.csv(file, header=True, schema=self.schema) for file in train_files]

        # Merge all DataFrames into one
        merged_df = reduce(DataFrame.unionByName, dfs)
        self.train_df = merged_df
        
    def _clean_train_data(self):
        self._remove_duplicate_rows()
        self._drop_dirty_data()
        
    def _remove_duplicate_rows(self):
        # Delete duplicate rows
        duplicate_rows = self.train_df.count() - self.train_df.dropDuplicates().count()
        self.train_df.dropDuplicates()
        print(f"Removed {duplicate_rows} duplicate rows.")
        
    def _drop_dirty_data(self):
        # Ensuring categorical values can only have correct values
        self.train_df = self.train_df.withColumn("vine", when(self.train_df["vine"].isin('Y', 'N'), self.train_df["vine"]).otherwise(None))
        self.train_df = self.train_df.withColumn("verified_purchase", when(self.train_df["verified_purchase"].isin('Y', 'N'), self.train_df["verified_purchase"]).otherwise(None))
        self.train_df = self.train_df.withColumn("label", when(self.train_df["label"].isin('True', 'False'), self.train_df["label"]).otherwise(None))
        
        # Select columns with categorical values
        columns_to_check = ["vine", "verified_purchase", "label"]

        # Remove rows where specified columns contain null or NaN values
        self.train_df = self.train_df.filter(
            (col(columns_to_check[0]).isNotNull()) &
            (col(columns_to_check[1]).isNotNull()) &
            (col(columns_to_check[2]).isNotNull())
        )
        
        
    


    