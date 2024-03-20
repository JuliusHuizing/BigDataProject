import yaml
from .DataQualityCheckModuleProtocol import DataQualityCheckModule
from .GXDataQualityCheckModule import GXDataQualityCheckModule
class DataQualityCheckModuleFactory:
    @staticmethod
    def create_module(module_name: str, module_config: dict) -> DataQualityCheckModule:
        try:
            if module_name == "GXDataQualityCheckModule":
                return GXDataQualityCheckModule(**module_config)
            else:
                raise ValueError(f"Invalid module name: {module_name}")
        except Exception as e:
            raise ValueError(f"Error creating module: {e}")
        
    
        
        
        
