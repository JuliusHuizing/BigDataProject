from Pipelines.Preprocess.PreprocessingModuleFactory import PreprocessingModuleFactory
from Pipelines.Collect.DataCollectorFactory import DataCollectorFactory
from Pipelines.Train.TrainPipelineFactory import TrainPipelineFactory
from Pipelines.Predict.PredictPipelineFactory import PredictPipelineFactory
from Pipelines.Predict.PredictPipeline import PredictPipeline
import yaml

from utils.initialize_modules_from_yaml import load_yaml_file, initialize_classes


if __name__ == "__main__":
    config = load_yaml_file('train_config.yaml')
    data_loader = DataCollectorFactory.create_module(config["collect"]["module"], config["collect"]["config"])
    
    preprocessing_pipeline = initialize_classes(config["preprocess"])

    train_pipeline = TrainPipelineFactory.create_module(config["train"]["module"], config["train"]["config"])

    # train
    df = data_loader.collect_data()[0]
    for module in preprocessing_pipeline:
        df = module.process(df)
    _ = train_pipeline.train(df)
    # train_pipeline.load_and_evaluate_model()
    
    
    
    
    
    
    

        
        
        
        
            
            
            
        
    
    
    