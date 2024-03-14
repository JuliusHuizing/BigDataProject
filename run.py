from Pipelines.PipelineModuleFactory import PipelineModuleFactory
import yaml
        
if __name__ == "__main__":
# Load the YAML configuration file
    with open("config.yaml", "r") as file:
        config = yaml.safe_load(file)
        
        for preprocessing_phase in config["preprocess"]:
            print(preprocessing_phase)
            for module_dict in config["preprocess"][preprocessing_phase]:
                module_name = module_dict["module"]
                module_config = module_dict["config"]
                module = PipelineModuleFactory.create_module(module_name, module_config)

                
                # module_config = preprocessing_phase[module_name]
                # module.run()
        
        
        
        
        
            
            
            
        
    
    
    