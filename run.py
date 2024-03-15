from Pipelines.PipelineModuleFactory import PipelineModuleFactory
import yaml

import yaml
import importlib

def load_yaml_file(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

def initialize_classes(config):
    # If config is a list, iterate through each item
    if isinstance(config, list):
        for item in config:
            initialize_classes(item)
    # If config is a dictionary, check for "module"
    elif isinstance(config, dict):
        if "module" in config and "config" in config:
            # module_name, class_name = config["module"].rsplit(".", 1)
            module_name = config["module"]
            config = config["config"]
            print(module_name, config)
            PipelineModuleFactory.create_module(module_name, config)
            # module = importlib.import_module(module_name)
            # cls = getattr(module, class_name)
            # # Instantiate the class with its config
            # instance = cls(**config["config"])
            # print(f"Initialized {class_name} with config: {config['config']}")
            # Optionally, you can do something with the instance here
        else:
            # Recursively search for nested configurations
            for key in config:
                initialize_classes(config[key])



        
if __name__ == "__main__":
    # Assuming your YAML file path is 'path/to/your/config.yaml'
    config = load_yaml_file('config.yaml')
    initialize_classes(config)
    
# # Load the YAML configuration file
#     with open("config.yaml", "r") as file:
#         config = yaml.safe_load(file)
        
#         data_path = config["collect"]["path"]
#         DataLoader = PipelineModuleFactory.create_module(module_name, module_config)
        
#         for preprocessing_phase in config["preprocess"]:
#             print(preprocessing_phase)
#             for module_dict in config["preprocess"][preprocessing_phase]:
#                 module_name = module_dict["module"]
#                 module_config = module_dict["config"]
#                 module = PipelineModuleFactory.create_module(module_name, module_config)
                
#         for preprocessing_phase in config["preprocess"]:
#             print(preprocessing_phase)
#             for module_dict in config["preprocess"][preprocessing_phase]:
#                 module_name = module_dict["module"]
#                 module_config = module_dict["config"]
#                 module = PipelineModuleFactory.create_module(module_name, module_config)

                
   
        
        
        
        
        
            
            
            
        
    
    
    