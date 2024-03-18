from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType
import logging

class DropNotIn:
    def __init__(self, input_column_name: str, output_column_name: str, targets: list[any]):
 
        self.input_column_name = input_column_name
        self.output_column_name = output_column_name
        self.targets = targets

    def process(self, df: DataFrame) -> DataFrame:
        # drop all rows that are not in targets
        original_count = df.count()
        df = df.filter(col(self.input_column_name).isin(self.targets))
        new_count = df.count()
        diff = original_count - new_count
        if new_count > 0:
            logging.warning(f" 🟠 dropped {diff}/{original_count} rows.")
        return df 
     
