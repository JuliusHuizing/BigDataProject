from pyspark.sql import SparkSession
from pyspark.ml import Pipeline as SparkPipeline, PipelineModel
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import os
from pyspark.sql import DataFrame
import logging
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator

class TrainPipeline:
    def __init__(self, features: list, num_trees: int, target: str, train_split: float, save_dir: str, overwrite: bool = False):
        self.spark = SparkSession.builder.appName("RandomForestExample").getOrCreate()
        self.feature_columns = features
        self.target = target
        self.train_split = train_split
        self.test_split = 1 - train_split
        self.overwrite = overwrite
        self.save_dir = save_dir
        self.num_trees = num_trees
        
    def train(self, df: DataFrame) -> str:
        self.df = df
        self.prepare_data()
        self.configure_and_train_model()
        self.save_model()
        return self.save_dir
        # self.load_and_evaluate_model()
    
    def prepare_data(self):
        # Split the data
        (self.training_data, self.test_data) = self.df.randomSplit([self.train_split, self.test_split])
        
        # Assemble the feature columns into a single feature vector
        self.assembler = VectorAssembler(inputCols=self.feature_columns, outputCol="features")
        
        # If "label" is not a numeric column, convert it to numeric using StringIndexer
        self.label_indexer = StringIndexer(inputCol=self.target, outputCol="indexedTarget").fit(self.df)

    def configure_and_train_model(self):
         # Configure the Random Forest model
        rf = RandomForestClassifier(labelCol="indexedTarget", featuresCol="features")
        
        # Chain indexers and forest in a Pipeline
        pipeline = SparkPipeline(stages=[self.label_indexer, self.assembler, rf])
        
        # Define parameter grid
        paramGrid = ParamGridBuilder()\
            .addGrid(rf.numTrees, [10, 20, 30]).addGrid(rf.maxDepth, [5, 10, 20]).build()
        
        # Define evaluator
        evaluator = BinaryClassificationEvaluator(labelCol="indexedTarget")
        
        # Configure CrossValidator
        cv = CrossValidator(estimator=pipeline,
                            estimatorParamMaps=paramGrid,
                            evaluator=evaluator,
                            numFolds=5)  # Example with 5-fold cross-validation
        
        # Run cross-validation, and choose the best set of parameters.
        self.cvModel = cv.fit(self.training_data)
        self.model = self.cvModel.bestModel
        print("\n\n\n\n\n\n\n\n\n")
        print(self.cvModel)

        # # Configure the Random Forest model
        # rf = RandomForestClassifier(labelCol="indexedTarget", featuresCol="features", numTrees=self.num_trees)
        
        # # Chain indexers and forest in a Pipeline
        # pipeline = SparkPipeline(stages=[self.label_indexer, self.assembler, rf])
        
        # # Train model
        # self.model = pipeline.fit(self.training_data)
        
        # # Save the trained model
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
        logging.info(f"🎯 Model Accuracy: {accuracy}")

    def process(self, df: DataFrame) -> None:
        self.df = df
        self.prepare_data()
        self.configure_and_train_model()
        self.save_model()
        self.load_and_evaluate_model()
        

       
