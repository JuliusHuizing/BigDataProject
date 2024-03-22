from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql import DataFrame

class OneHotEncodingModule:
    def __init__(self, input_column_name: str, output_column_name: str = None):
        """
        Initializes the feature engineering module to apply one-hot encoding to a specified column.

        Parameters:
        - input_column_name: The name of the column to encode.
        - output_column_name: The name for the output column where the encoded feature will be stored. 
                              If None, a default name will be generated.
        """
        self.input_column_name = input_column_name
        self.output_column_name = output_column_name if output_column_name else f"{input_column_name}_encoded"

    def process(self, data: DataFrame) -> DataFrame:
        """
        Processes the input DataFrame to apply one-hot encoding to the specified column.

        Parameters:
        - data: A PySpark DataFrame containing the data to process.

        Returns:
        - A PySpark DataFrame with an additional column for the one-hot encoded feature.
        """
        # Generate a unique name for the intermediate indexed column
        indexed_column_name = f"{self.input_column_name}_indexed"

        # Define the stages for the pipeline: indexing and encoding
        indexer = StringIndexer(inputCol=self.input_column_name, outputCol=indexed_column_name, handleInvalid='keep')
        encoder = OneHotEncoder(inputCols=[indexed_column_name], outputCols=[self.output_column_name])
        stages = [indexer, encoder]

        # Build and apply the pipeline
        pipeline = Pipeline(stages=stages)
        model = pipeline.fit(data)
        return model.transform(data)
