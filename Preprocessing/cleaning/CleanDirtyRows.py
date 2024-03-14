from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col
class CleanDirtyRows:
    def __init__(self, columns_to_check=None):
        """
        Initializes the module to drop rows with dirty data in specified columns.

        Parameters:
        - columns_to_check: A list of column names to check for cleanliness. If None, a default list of columns
                            ["vine", "verified_purchase", "label"] is used.
        """
        if columns_to_check is None:
            self.columns_to_check = ["vine", "verified_purchase", "label"]
        else:
            self.columns_to_check = columns_to_check

    def process(self, df: DataFrame) -> DataFrame:
        """
        Drops rows with dirty data from the PySpark DataFrame based on the specified columns.

        Parameters:
        - df: The PySpark DataFrame to clean.

        Returns:
        - The cleaned PySpark DataFrame.
        """
        for column in self.columns_to_check:
            df = df.withColumn(column, when(col(column).isin('Y', 'N'), col(column)).otherwise(None))
            df = df.filter(col(column).isNotNull())
        return df
