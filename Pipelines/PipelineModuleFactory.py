import yaml
from .Preprocess.feature_engineering.TextLanguageFeature import TextLanguageFeature
from .Preprocess.feature_engineering.TextLengthFeature import TextLengthFeature
from .Preprocess.feature_engineering.TextSentimentFeature import TextSentimentFeature
from .Preprocess.collection.DataLoader import DataLoader
from .Train.TrainPipeline import TrainPipeline
class PipelineModuleFactory:
    @staticmethod
    def create_module(module_name: str, module_config: dict):
        try:
            if module_name == "TrainPipeline":
                return TrainPipeline(**module_config)
            if module_name == "DataLoader":
                return DataLoader(**module_config)
            if module_name == "TextLanguageFeature":
                return TextLanguageFeature(**module_config)
            elif module_name == "TextLengthFeature":
                return TextLengthFeature(**module_config)
            elif module_name == "TextSentimentFeature":
                return TextSentimentFeature(**module_config)
            else:
                raise ValueError(f"Invalid module name: {module_name}")
        except Exception as e:
            raise ValueError(f"Error creating module: {e}")
        
    
        
        
        
