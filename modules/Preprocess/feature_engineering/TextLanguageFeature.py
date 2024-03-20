from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from langdetect import detect

class TextLanguageFeature:
    def __init__(self, input_column_name: str, output_column_name: str = "detected_language"):
        """
        Initializes the feature engineering module to detect language from text in a specified column.

        Parameters:
        - input_column_name: The name of the column containing the text to analyze.
        - output_column_name: The name of the output column where the detected language will be stored. 
                              Defaults to "detected_language".
        """
        self.input_column_name = input_column_name
        self.output_column_name = output_column_name

    def process(self, data: DataFrame) -> DataFrame:
        """
        Processes the input DataFrame to detect the language of text in the specified column.

        Parameters:
        - data: A PySpark DataFrame containing the text data to analyze.

        Returns:
        - A PySpark DataFrame with an additional column for the detected language.
        """
        detectLanguageUDF = udf(lambda x: self._detect_language(x), StringType())
        return data.withColumn(self.output_column_name, detectLanguageUDF(col(self.input_column_name)))

    def _detect_language(self, text: str) -> str:
        """
        Detects the language of a given piece of text.

        Parameters:
        - text: The text string to analyze.

        Returns:
        - The detected language as a string, or "unknown language" if detection fails.
        """
        try:
            return detect(text)
        except Exception:
            return "unknown language"

        
        
        
        
        
