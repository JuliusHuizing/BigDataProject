
from .DataCollectorProtocol import DataCollectorProtocol
from .DataLoader import DataLoader
class DataCollectorFactory:
    @staticmethod
    def create_module(module_name: str, module_config: dict) -> DataCollectorProtocol:
        try:
            if module_name == "DataLoader":
                return DataLoader(**module_config)
            else:
                raise ValueError(f"Invalid module name: {module_name}")
           
        except Exception as e:
            raise ValueError(f"Error creating module: {e}")
        
    
        
        
        
