from pyspark.sql import SparkSession
from pyspark.ml import Pipeline as SparkPipeline, PipelineModel
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import os
from pyspark.sql import DataFrame

class TrainPipeline:
    def __init__(self, df: DataFrame):
        self.spark = SparkSession.builder.appName("RandomForestExample").getOrCreate()
        self.df = df
        self.feature_columns = ["review_body_length", "review_header_length"]
        self.model_path = "model"  # Assuming 'Config.MODEL_PATH' is equivalent to this placeholder
    
    def prepare_data(self):
        # Split the data
        (self.training_data, self.test_data) = self.df.randomSplit([0.7, 0.3])
        
        # Assemble the feature columns into a single feature vector
        self.assembler = VectorAssembler(inputCols=self.feature_columns, outputCol="features")
        
        # If "label" is not a numeric column, convert it to numeric using StringIndexer
        self.label_indexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(self.df)

    def configure_and_train_model(self):
        # Configure the Random Forest model
        rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="features", numTrees=10)
        
        # Chain indexers and forest in a Pipeline
        pipeline = SparkPipeline(stages=[self.label_indexer, self.assembler, rf])
        
        # Train model
        self.model = pipeline.fit(self.training_data)
        
        # Save the trained model
        self.save_model()

    def save_model(self):
        # Check if model path exists and delete it if it does
        if os.path.exists(self.model_path):
            os.system(f"rm -rf {self.model_path}")
        self.model.save(self.model_path)
    
    def load_and_evaluate_model(self):
        # Load the model
        model = PipelineModel.load(self.model_path)
        
        # Transform the test data
        predictions = model.transform(self.test_data)
        
        # Show example predictions
        # predictions.select("prediction", "indexedLabel", "features").show(5)
        
        # Evaluate the model
        evaluator = BinaryClassificationEvaluator(labelCol="indexedLabel")
        accuracy = evaluator.evaluate(predictions)
        print(f"Accuracy: {accuracy}")

# # Example of using the TrainPipeline class
# if __name__ == "__main__":
#     # Initialize SparkSession (this should be done inside the main guard)
#     spark = SparkSession.builder.appName("TrainPipelineExample").getOrCreate()
    
    # Example DataFrame loading/creation
    # df = spark.read.csv("path/to/your/data.csv", header=True, inferSchema=True)
    
    # Initialize the training pipeline with the DataFrame
    # train_pipeline = TrainPipeline(df)
    
    # Prepare data
    # train_pipeline.prepare_data()
    
    # Configure and train the model
    # train_pipeline.configure_and_train_model()
    
    # Load and evaluate the trained model
    # train_pipeline.load_and_evaluate_model()
