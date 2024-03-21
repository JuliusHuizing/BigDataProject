from modules.Preprocess.PreprocessingModuleFactory import PreprocessingModuleFactory
from modules.Collect.DataCollectorFactory import DataCollectorFactory
from modules.Train.TrainPipelineFactory import TrainPipelineFactory
from modules.Predict.PredictPipelineFactory import PredictPipelineFactory
from modules.Predict.PredictPipeline import PredictPipeline
from modules.Check.DataQualityCheckModuleFactory import DataQualityCheckModuleFactory
import yaml
from DataChecker import DataChecker
from modules.Preprocess.integration.DataJoiner import DataJoiner
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
        
        logging.info("Initializing raw data quality check pipeline...")
        raw_data_quality_check_module = DataQualityCheckModuleFactory\
            .create_module(config["raw_data_quality_check"]["module"], 
                           config["raw_data_quality_check"]["config"])
        logging.info("✅ Raw data quality check pipeline initialized.")
        
        logging.info("Initializing preprocessing pipeline...")
        
        # isolate integration step as separate phaase
        integration_phase = config["preprocess"]["integrate"]
        
        integration_data_loader = DataCollectorFactory.create_module(integration_phase["collect"]["module"], integration_phase["collect"]["config"])
        integration_preprocessing_pipeline = initialize_classes(integration_phase["preprocess"])
        
        join_config = integration_phase["join"]
        integration_pipeline = DataJoiner(collect=integration_data_loader, preprocess = integration_preprocessing_pipeline, column_primary_source=join_config["column_primary_source"], column_secondary_source=join_config["column_secondary_source"], join_type=join_config["join_type"])
        # integratoin_pipeline = initialize_classes(integration_phase)
        integration_pipeline = initialize_classes(integration_phase["preprocess"])
        
        # remove it from the config to avoid problems down the line
        del config["preprocess"]["integrate"]
        
        preprocessing_pipeline = initialize_classes(config["preprocess"])
        
        logging.info("✅ Preprocessing pipeline initialized.")
        
        logging.info("Initializing pre-trainining data quality check pipeline...")
        pre_train_data_quality_check_pipeline = DataQualityCheckModuleFactory\
            .create_module(config["pre_training_data_quality_check"]["module"], 
                           config["pre_training_data_quality_check"]["config"])
        logging.info("✅ Pre-training data quality check pipeline initialized.")

        logging.info("Initializing training pipeline...")
        train_pipeline = TrainPipelineFactory.create_module(config["train"]["module"], config["train"]["config"])
        logging.info("✅ Training pipeline initialized.")

        # train
        logging.info("Collecting Data...")
        df = data_loader.collect_data()[0]
        # original_count = df.count()
        
        logging.info("✅ Data collected.")
        
        logging.info("Checking raw data quality...")
        df = raw_data_quality_check_module.process(df)
        logging.info("✅ Raw data quality checked.")
        
        
        
        logging.info("Cleaning Data...")
        for module in preprocessing_pipeline:
            original_count = df.count()
            df = module.process(df)
            cleaned_count = df.count()
            diff = original_count - cleaned_count
            if diff > 0:
                logging.warning(f"🟠 Dropped {diff} rows in {module}.")
        logging.info("✅ Data Preprocessed.")
       
        logging.info("Checking preprocessed data quality before training...")
        DataChecker().check_data(df)
        df = pre_train_data_quality_check_pipeline.process(df)
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
    
    
    
    
    
    
    
    

        
        
        
        
            
            
            
        
    
    
    