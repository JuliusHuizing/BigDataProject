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
    def __init__(self, features: list, target: str, train_split: float, save_dir: str, overwrite: bool = False, grid_search: dict = None):
        self.spark = SparkSession.builder.appName("RandomForestExample").getOrCreate()
        self.feature_columns = features
        self.target = target
        self.train_split = train_split
        self.test_split = 1 - train_split
        self.overwrite = overwrite
        self.save_dir = save_dir
        self.grid_search = grid_search  # Store grid search params and k value
    
    def train(self, df: DataFrame) -> str:
        self.df = df
        self.prepare_data()
        self.configure_and_train_model()
        self.save_model()
        return self.save_dir
    
    def prepare_data(self):
        (self.training_data, self.test_data) = self.df.randomSplit([self.train_split, self.test_split])
        self.assembler = VectorAssembler(inputCols=self.feature_columns, outputCol="features")
        self.label_indexer = StringIndexer(inputCol=self.target, outputCol="indexedTarget").fit(self.df)

    def configure_and_train_model(self):
        rf = RandomForestClassifier(labelCol="indexedTarget", featuresCol="features")
        pipeline = SparkPipeline(stages=[self.label_indexer, self.assembler, rf])

        if self.grid_search:
            paramGrid = ParamGridBuilder()
            for param, values in self.grid_search['params'].items():
                paramGrid = paramGrid.addGrid(eval(f"rf.{param}"), values)
            paramGrid = paramGrid.build()

            evaluator = BinaryClassificationEvaluator(labelCol="indexedTarget")
            cv = CrossValidator(estimator=pipeline,
                                estimatorParamMaps=paramGrid,
                                evaluator=evaluator,
                                numFolds=self.grid_search['k'])  # Use k from grid_search
            self.cvModel = cv.fit(self.training_data)
            self.model = self.cvModel.bestModel
        else:
            self.model = pipeline.fit(self.training_data)

    def save_model(self):
        if os.path.exists(self.save_dir):
            if self.overwrite:
                os.system(f"rm -rf {self.save_dir}")
            else:
                logging.warning(f"Model already exists at {self.save_dir}. Use overwrite=True to overwrite it.")
                return
        self.model.save(self.save_dir)

    def load_and_evaluate_model(self):
        model = PipelineModel.load(self.save_dir)
        predictions = model.transform(self.test_data)
        evaluator = BinaryClassificationEvaluator(labelCol="indexedTarget")
        accuracy = evaluator.evaluate(predictions)
        logging.info(f"🎯 Model Accuracy: {accuracy}")

    def process(self, df: DataFrame) -> None:
        self.df = df
        self.prepare_data()
        self.configure_and_train_model()
        self.save_model()
        self.load_and_evaluate_model()
