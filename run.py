

# import yaml
# import importlib
# import os

# def load_yaml_file(file_path):
#     with open(file_path, 'r') as file:
#         return yaml.safe_load(file)

# def initialize_classes(config):
#     modules = []
#     # If config is a list, iterate through each item
#     if isinstance(config, list):
#         for item in config:
#             modules.extend(initialize_classes(item))  # Aggregate results from recursive calls

#     # If config is a dictionary, check for "module"
#     elif isinstance(config, dict):
#         if "module" in config and "config" in config:
#             module_name = config["module"]
#             config = config["config"]
#             # print(module_name, config)
#             # although we could use importlib, we will use a factory pattern instead
#             # because python relative imports are a just a pain.
#             module = PreprocessingModuleFactory.create_module(module_name, config)
#             modules.append(module)
#         else:
#             # Recursively search for nested configurations
#             for key in config:
#                 modules.extend(initialize_classes(config[key]))
                
#     return modules

# if __name__ == "__main__":
    
    
#     # Assuming your YAML file path is 'path/to/your/config.yaml'
#     config = load_yaml_file('config.yaml')
#     train_config = config["train"]
#     data_loader = DataCollectorFactory.create_module(train_config["collect"]["module"], train_config["collect"]["config"])
#     preprocessing_pipeline = initialize_classes(train_config["preprocess"])
#     clean_pipeline = initialize_classes(train_config["clean"])
#     train_pipeline = TrainPipelineFactory.create_module(train_config["train"]["module"], train_config["train"]["config"])

#     predict_config = config["predict"]
#     predict_data_dir = predict_config["collect"]["config"]["data_dir"]
#     prediction_files = [file for file in os.listdir(predict_data_dir) if file != "schema.yaml"]
#     predictions_dir = predict_config["save_dir"]
#     predict_data_loader = DataCollectorFactory.create_module(predict_config["collect"]["module"], predict_config["collect"]["config"])
#     # predict_pipeline = PredictPipelineFactory.create_module(config["predict"]["module"], config["predict"]["config"])

#     # train
#     df = data_loader.collect_data()[0]
#     for module in clean_pipeline:
#         df = module.process(df)
#     for module in preprocessing_pipeline:
#         df = module.process(df)
#     model_path = train_pipeline.train(df)
#     # train_pipeline.load_and_evaluate_model()
    
#     # # predict
#     dfs = predict_data_loader.collect_data()
#     predict_pieline = PredictPipeline(model_path)
#     predict_preprocessing_pipeline = initialize_classes(train_config["preprocess"])
#     for source_file, df in zip(prediction_files, dfs):
#         for module in predict_preprocessing_pipeline:
#             df = module.process(df)
#         predict_pieline.predict(df, results_path=f"{predictions_dir}preds_{source_file}")
    
    
    
    
    
    
    
    

        
        
        
        
            
            
            
        
    
    
    