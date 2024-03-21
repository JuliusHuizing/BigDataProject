from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType
import logging

class RenameColumns:
    def __init__(self, mapping: dict):
 
        self.mapping = mapping

    def process(self, df: DataFrame) -> DataFrame:
        for current_name, new_name in self.mapping.items():
            df = df.withColumnRenamed(current_name, new_name)
        return df
     
