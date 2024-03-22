from pyspark.ml.feature import StringIndexer, FeatureHasher
from pyspark.ml import Pipeline
from pyspark.sql import DataFrame

class HashingModule:
    def __init__(self, input_column_name: str, output_column_name: str = None, num_features: int = 20):
        """
        Initializes the module to apply feature hashing to a specified column.

        Parameters:
        - input_column_name: The name of the column to encode.
        - output_column_name: The name for the output column where the hashed feature will be stored. 
                              If None, a default name will be generated.
        - numFeatures: The number of features (dimensions) in the output feature vector.
        """
        self.input_column_name = input_column_name
        self.output_column_name = output_column_name if output_column_name else f"{input_column_name}_hashed"
        self.numFeatures = num_features

    def process(self, data: DataFrame) -> DataFrame:
        """
        Processes the input DataFrame to apply feature hashing to the specified column.

        Parameters:
        - data: A PySpark DataFrame containing the data to process.

        Returns:
        - A PySpark DataFrame with an additional column for the hashed feature.
        """
        # Define the feature hasher
        hasher = FeatureHasher(inputCols=[self.input_column_name], outputCol=self.output_column_name, numFeatures=self.numFeatures, categoricalCols=[self.input_column_name])

        # Transform the data
        return hasher.transform(data)
