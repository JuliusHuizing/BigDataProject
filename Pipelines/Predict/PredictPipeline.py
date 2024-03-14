from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import initcap
from Config import Config

class PredictPipeline:
    def __init__(self):
        self.spark = SparkSession.builder.appName("PredictionPipeline").getOrCreate()
        self.model = PipelineModel.load(Config.MODEL_PATH)
    
    def load_data(self, split_type):
        # Assuming DataLoader is a custom class for loading data according to the split (validation or test)
        return DataLoader(split_type).data

    def preprocess_data(self, df):
        # This method is a placeholder. Assuming `val_preprocessing_pipeline` and `test_preprocessing_pipeline`
        # are instances of a preprocessing pipeline that need to be defined or passed to this class.
        # For now, it simply returns the input DataFrame for the sake of completeness.
        return df  # Placeholder for actual preprocessing

    def predict_and_save(self, df, prediction_path, file_name):
        predictions = self.model.transform(df)
        # Convert predictions to boolean and then to string, and capitalize the first letter
        predictions = predictions.withColumn("label", initcap(predictions["prediction"].cast("boolean").cast("string")))
        predictions.select("label").write.csv(f'{prediction_path}/{file_name}', mode="overwrite", header=False)

    def run(self):
        # Load validation and test data
        val_data = self.load_data(DataSplit.VALIDATION)
        test_data = self.load_data(DataSplit.TEST)

        # Preprocess data (assuming preprocessing logic is defined elsewhere)
        val_df = self.preprocess_data(val_data)
        test_df = self.preprocess_data(test_data)

        # Make predictions and save them
        self.predict_and_save(val_df, Config.PREDICTIONS_PATH, Config.VALIDATION_PREDICTIONS_NAME)
        self.predict_and_save(test_df, Config.PREDICTIONS_PATH, Config.TEST_PREDICTIONS_NAME)

# Assuming Config and DataLoader are defined and configured properly, along with DataSplit and DataSet.
# The following line would instantiate and run the pipeline:
# pipeline = PredictPipeline()
# pipeline.run()
