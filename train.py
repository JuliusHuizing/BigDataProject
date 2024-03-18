from Pipelines.Preprocess.PreprocessingModuleFactory import PreprocessingModuleFactory
from Pipelines.Collect.DataCollectorFactory import DataCollectorFactory
from Pipelines.Train.TrainPipelineFactory import TrainPipelineFactory
from Pipelines.Predict.PredictPipelineFactory import PredictPipelineFactory
from Pipelines.Predict.PredictPipeline import PredictPipeline
import yaml
from DataChecker import DataChecker

from utils.initialize_modules_from_yaml import load_yaml_file, initialize_classes

import logging
if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    try:
        logging.info("Loading configuration...")
        config = load_yaml_file('train_config.yaml')
        logging.info("✅ Configuration loaded.")
        
        logging.info("Initializing data loader...")
        data_loader = DataCollectorFactory.create_module(config["collect"]["module"], config["collect"]["config"])
        logging.info("✅ Data loader initialized.")
        
        logging.info("Initializing preprocessing pipeline...")
        preprocessing_pipeline = initialize_classes(config["preprocess"])
        logging.info("✅ Preprocessing pipeline initialized.")

        logging.info("Initializing training pipeline...")
        train_pipeline = TrainPipelineFactory.create_module(config["train"]["module"], config["train"]["config"])
        logging.info("✅ Training pipeline initialized.")

        # train
        logging.info("Collecting Data...")
        df = data_loader.collect_data()[0]
        # original_count = df.count()
        
        logging.info("✅ Data collected.")
        logging.info("Cleaning Data...")
        for module in preprocessing_pipeline:
            df = module.process(df)
        logging.info("✅ Data Preprocessed.")
        # new_count = df.count()
        # num_deleted_rows = original_count - new_count
        # if num_deleted_rows > 0
        
        logging.info("Checking preprocessed data quality before training...")
            
        DataChecker().check_data(df)
        logging.info("✅ Data seems ready for training.")
             
        logging.info("Starting training...")
        _ = train_pipeline.train(df)
        logging.info(f"✅ Model trained.")
        
        logging.info("Loading and evaluating model on held out test set...")
        train_pipeline.load_and_evaluate_model()
        logging.info(f"✅ Model evaluated.")
        
    except Exception as e:
        logging.error(f" ❌ Error in pipeline: {e}")
        raise e
    
    # train_pipeline.load_and_evaluate_model()
    
    
    
    
    
    
    

        
        
        
        
            
            
            
        
    
    
    