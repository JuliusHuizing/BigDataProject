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
import yaml
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

import pandas as pd
import os
from enum import Enum

from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnan, when, count
from pyspark.ml.feature import Imputer, StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import os

import logging

class JSONDataLoader:
    def __init__(self, data_dir: str, merge: bool, n: int = None):
        # Create a SparkSession
        self.spark = SparkSession.builder \
            .appName("JSONDataLoader") \
            .getOrCreate()
        # self._create_schema()
        self.merge = merge
        self.data_dir = data_dir
        self.n = n
        self.create_schema()
        
    def collect_data(self) -> list[DataFrame]:
        # List of file paths for training data
        files = []
        for file in os.listdir(self.data_dir):
            if file.endswith('.json'):
                files.append(self.data_dir + file)
            else:
                if file != "schema.yaml":
                    logging.warning(f"File {file} is not a JSON file and will be ignored.")
        self.dfs = [self.spark.read.json(file, schema=self.schema) for file in files]
        # if n is set, collect subset of data
        if self.n != None:
            n = int(self.n/len(self.dfs))
            self.dfs = [df.limit(n) for df in self.dfs]
            
        # Merge all DataFrames into one
        if self.merge:
            merged_df = reduce(DataFrame.unionByName, self.dfs)
            self.dfs = [merged_df]
        return self.dfs

    def create_schema(self):
        # Define a dictionary to map the types from the YAML file to PySpark types
        type_mapping = {
            "IntegerType": IntegerType(),
            "StringType": StringType()
        }

        # Load the schema from the YAML file
        with open(f"{self.data_dir}schema.yaml", "r") as file:
            schema_yaml = yaml.safe_load(file)

        # Dynamically construct the StructType based on the YAML content
        schema_fields = []
        for field in schema_yaml["fields"]:
            field_type = type_mapping[field["type"]]
            schema_fields.append(StructField(field["name"], field_type, field["nullable"]))

        self.schema = StructType(schema_fields)
        # logging.info(f"Schema created: {self.schema}")





