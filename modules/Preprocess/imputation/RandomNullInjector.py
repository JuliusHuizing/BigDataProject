from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, rand

class RandomNullInjector:
    def __init__(self, input_column_name: str, output_column_name: str, null_percentage: float):
        """
        Initializes the module to randomly set values of a specified column to null in a new output column,
        based on a given percentage.

        Parameters:
        - input_column_name: The name of the column to read values from.
        - output_column_name: The name of the new column to create, which will contain the original values
                              with a certain percentage set to null.
        - null_percentage: The percentage of the column's values to randomly set as null.
        """
        self.input_column_name = input_column_name
        self.output_column_name = output_column_name
        self.null_percentage = null_percentage

    def process(self, data: DataFrame) -> DataFrame:
        """
        Processes the input DataFrame to create a new column with randomly injected null values,
        based on the original column values.

        Parameters:
        - data: A PySpark DataFrame containing the data to process.

        Returns:
        - A PySpark DataFrame with a new column added that contains randomly injected null values.
        """
        # Calculate the threshold for setting a value to null based on the given percentage
        threshold = 1 - self.null_percentage

        # Generate a random value between 0 and 1 for each row in the new column,
        # set to null if the random value is above the threshold, otherwise copy the original column value
        return data.withColumn(self.output_column_name, when(rand() >= threshold, lit(None)).otherwise(col(self.input_column_name)))
