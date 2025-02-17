import yaml
from .feature_engineering.TextLanguageFeature import TextLanguageFeature
from .feature_engineering.TextLengthFeature import TextLengthFeature
from .feature_engineering.TextSentimentFeature import TextSentimentFeature
from .PreprocessingModuleProtocol import PreprocessingModule
from .cleaning.DropNull import DropNull
from .cleaning.ConvertToBool import ConvertToBoolean
from .cleaning.DropNotIn import DropNotIn
from .cleaning.RenameColumns import RenameColumns
from .integration.DataJoiner import DataJoiner
from .imputation.ImputationModule import ImputationModule
from .transformation.OneHotEncodingModule import OneHotEncodingModule
from .imputation.RandomNullInjector import RandomNullInjector
from .feature_engineering.CharacterCountModule import CharacterCountModule
from .transformation.HashingModule import HashingModule
class PreprocessingModuleFactory:
    @staticmethod
    def create_module(module_name: str, module_config: dict) -> PreprocessingModule:
        try:
            if module_name == "HashingModule":
                return HashingModule(**module_config)
            if module_name == "CharacterCountModule":
                return CharacterCountModule(**module_config)
            if module_name == "RandomNullInjector":
                return RandomNullInjector(**module_config)
            if module_name == "OneHotEncodingModule":
                return OneHotEncodingModule(**module_config)
            if module_name == "ImputationModule":
                return ImputationModule(**module_config)
            if module_name == "DataJoiner":
                return DataJoiner(**module_config)
            if module_name == "RenameColumns":
                return RenameColumns(**module_config)
            if module_name == "DropNotIn":
                return DropNotIn(**module_config)
            if module_name == "ConvertToBoolean":
                return ConvertToBoolean(**module_config)
            if module_name == "DropNull":
                return DropNull(**module_config)
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
        
    
        
        
        
