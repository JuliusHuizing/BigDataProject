from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col

class DropNull:
    def __init__(self, columns: list[str]):
        """
        Initializes the module to drop rows with dirty data in specified columns.

        Parameters:
        - columns_to_check: A list of column names to check for cleanliness. If None, a default list of columns
                            ["vine", "verified_purchase", "label"] is used.
        """

        self.columns_to_check = columns

    def process(self, df: DataFrame) -> DataFrame:
        """
        Drops rows with dirty data from the PySpark DataFrame based on the specified columns.

        Parameters:
        - df: The PySpark DataFrame to clean.

        Returns:
        - The cleaned PySpark DataFrame.
        """
        for column in self.columns_to_check:
            df = df.filter(col(column).isNotNull())
        return df
