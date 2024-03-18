   
    
from Pipelines.Preprocess.PreprocessingModuleFactory import PreprocessingModuleFactory
from Pipelines.Collect.DataCollectorFactory import DataCollectorFactory
from Pipelines.Train.TrainPipelineFactory import TrainPipelineFactory
from Pipelines.Predict.PredictPipelineFactory import PredictPipelineFactory
from Pipelines.Predict.PredictPipeline import PredictPipeline
import yaml
import os
import logging

from utils.initialize_modules_from_yaml import load_yaml_file, initialize_classes


if __name__ == "__main__":
    logging.info("Starting prediction pipeline")
    try:
        config = load_yaml_file('predict_config.yaml')
        predict_data_dir = config["collect"]["config"]["data_dir"]
        prediction_files = [file for file in os.listdir(predict_data_dir) if file != "schema.yaml"]
        predictions_dir = config["predict"]["save_dir"]
        predict_data_loader = DataCollectorFactory.create_module(config["collect"]["module"], config["collect"]["config"])
        
        dfs = predict_data_loader.collect_data()
        predict_pieline = PredictPipeline(config["load_model"])
        predict_preprocessing_pipeline = initialize_classes(config["preprocess"])
        for source_file, df in zip(prediction_files, dfs):
            for module in predict_preprocessing_pipeline:
                df = module.process(df)
            predict_pieline.predict(df, results_path=f"{predictions_dir}preds_{source_file}")
    except Exception as e:
        logging.error(f"Error in prediction pipeline: {e}")
        raise e
        
        
        
            
            
            
        
    
    
    