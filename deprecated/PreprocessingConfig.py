from .cleaning.CleanDirtyRows import CleanDirtyRows
from .cleaning.CleanDuplicateRows import CleanDuplicateRows

from .feature_engineering.TextLanguageFeature import TextLanguageFeature
from .feature_engineering.TextLengthFeature import TextLengthFeature
from .feature_engineering.TextSentimentFeature import TextSentimentFeature
from .collection.DataLoader import DataLoader
from .collection.DataLoader import DataSplit

class PreprocessingConfig:
    DATA_COLLECTOR = DataLoader(split=DataSplit.TRAIN)
    DATA_CLEANING_MODULES = [
        # CleanDuplicateRows(),
        # CleanDirtyRows()

    ]
    FEATURE_ENGINEERING_MODULES = [
        TextLanguageFeature(input_column_name="review_body", output_column_name="review_language"),
        TextLengthFeature(input_column_name="review_body", output_column_name="review_body_length"),
        TextLengthFeature(input_column_name="review_headline", output_column_name="review_header_length"),
    ]
    PREPROCESSING_MODULES = DATA_CLEANING_MODULES + FEATURE_ENGINEERING_MODULES
    
    # STEPS = COLLECTION_STEPS + FEATURE_ENGINEERING_MODULES
        
        