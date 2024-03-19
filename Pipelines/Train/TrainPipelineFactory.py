import yaml
from .TrainPipeline import TrainPipeline
from .TrainPipelineProtocol import TrainingPipelineProtocol
class TrainPipelineFactory:
    @staticmethod
    def create_module(module_name: str, module_config: dict) -> TrainingPipelineProtocol:
        try:
            if module_name == "TrainPipeline":
                return TrainPipeline(**module_config)
            else:
                raise ValueError(f"Invalid module name: {module_name}")
        except Exception as e:
            raise ValueError(f"Error creating module: {e}")
        
    
        
        
        
