from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType

class TextLengthFeature:
    def __init__(self, input_column_name: str = "review_body", output_column_name: str = "review_length"):
        """
        Initializes the feature engineering module to calculate the length of the text in a specified column.

        Parameters:
        - input_column_name: The name of the column containing the text to analyze. Defaults to "review_body".
        - output_column_name: The name of the output column where the length will be stored. Defaults to "review_length".
        """
        self.input_column_name = input_column_name
        self.output_column_name = output_column_name

    def process(self, data: DataFrame) -> DataFrame:
        """
        Processes the input DataFrame to calculate the length of text in the specified column.

        Parameters:
        - data: A PySpark DataFrame containing the text data to analyze.

        Returns:
        - A PySpark DataFrame with an additional column for the text length.
        """
        lengthUDF = udf(lambda x: len(x) if x else 0, IntegerType())
        return data.withColumn(self.output_column_name, lengthUDF(col(self.input_column_name)))
