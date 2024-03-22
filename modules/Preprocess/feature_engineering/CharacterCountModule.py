from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType

class CharacterCountModule:
    def __init__(self, input_column_name: str, characters_to_count: list, output_column_name: str):
        """
        Initializes the feature engineering module to count occurrences of specified characters in a text column.

        Parameters:
        - input_column_name: The name of the column containing the text to analyze.
        - characters_to_count: A list of characters for which occurrences will be counted.
        - output_column_name: The name of the output column where the count will be stored.
        """
        self.input_column_name = input_column_name
        self.characters_to_count = set(characters_to_count)  # Use a set for faster lookup
        self.output_column_name = output_column_name

    def process(self, data):
        """
        Processes the input DataFrame to count occurrences of specified characters in the text column.

        Parameters:
        - data: A PySpark DataFrame containing the text data to analyze.

        Returns:
        - A PySpark DataFrame with an additional column for the count of specified characters.
        """
        # Define a UDF that counts occurrences of the specified characters
        count_characters_udf = udf(lambda x: sum(x.count(char) for char in self.characters_to_count if x), IntegerType())

        # Apply the UDF to the input column to create the output column with counts
        return data.withColumn(self.output_column_name, count_characters_udf(col(self.input_column_name)))
