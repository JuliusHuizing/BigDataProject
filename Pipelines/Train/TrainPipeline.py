from pyspark.sql import SparkSession
from pyspark.ml import Pipeline as SparkPipeline, PipelineModel
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import os
from pyspark.sql import DataFrame
import logging

class TrainPipeline:
    def __init__(self, features: list, target: str, train_split: float, save_dir: str, overwrite: bool = False):
        self.spark = SparkSession.builder.appName("RandomForestExample").getOrCreate()
        self.feature_columns = features
        self.target = target
        self.train_split = train_split
        self.test_split = 1 - train_split
        self.save_dir = save_dir
    
    def prepare_data(self):
        # Split the data
        (self.training_data, self.test_data) = self.df.randomSplit([self.train_split, self.test_split])
        
        # Assemble the feature columns into a single feature vector
        self.assembler = VectorAssembler(inputCols=self.feature_columns, outputCol="features")
        
        # If "label" is not a numeric column, convert it to numeric using StringIndexer
        self.label_indexer = StringIndexer(inputCol=self.target, outputCol="indexedTarget").fit(self.df)

    def configure_and_train_model(self):
        # Configure the Random Forest model
        rf = RandomForestClassifier(labelCol="indexedTarget", featuresCol="features", numTrees=10)
        
        # Chain indexers and forest in a Pipeline
        pipeline = SparkPipeline(stages=[self.label_indexer, self.assembler, rf])
        
        # Train model
        self.model = pipeline.fit(self.training_data)
        
        # Save the trained model
        self.save_model()

    def save_model(self):
        # Check if model path exists and delete it if it does
        if os.path.exists(self.save_dir):
            if self.overwrite:
                os.system(f"rm -rf {self.save_dir}")
            else :
                logging.warning(f"Model already exists at {self.save_dir}. Use overwrite=True to overwrite it.")
                return
        self.model.save(self.save_dir)
    
    def load_and_evaluate_model(self):
        # Load the model
        model = PipelineModel.load(self.save_dir)
        
        # Transform the test data
        predictions = model.transform(self.test_data)
        
        # Show example predictions
        # predictions.select("prediction", "indexedLabel", "features").show(5)
        
        # Evaluate the model
        evaluator = BinaryClassificationEvaluator(labelCol="indexedTarget")
        accuracy = evaluator.evaluate(predictions)
        print(f"Accuracy: {accuracy}")

    def process(self, df: DataFrame) -> None:
        self.df = df
        self.prepare_data()
        self.configure_and_train_model()
        self.save_model()
        self.load_and_evaluate_model()
        

       
