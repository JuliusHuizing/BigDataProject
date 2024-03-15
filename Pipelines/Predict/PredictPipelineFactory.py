from .PredictPipelineProtocol import PredictPipelineProtocol
from .PredictPipeline import PredictPipeline
class PredictPipelineFactory:
    @staticmethod
    def create_module(module_name: str, module_config: dict) -> PredictPipelineProtocol:
        try:
            if module_name == "PredictPipeline":
                return PredictPipeline(**module_config)
           
            else:
                raise ValueError(f"Invalid module name: {module_name}")
        except Exception as e:
            raise ValueError(f"Error creating module: {e}")
        
    
        
        
        
