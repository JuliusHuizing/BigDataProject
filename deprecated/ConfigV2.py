from  Pipelines.Preprocess.cleaning.CleanDirtyRows import CleanDirtyRows
from  Pipelines.Preprocess.cleaning.CleanDuplicateRows import CleanDuplicateRows

from  Pipelines.Preprocess.feature_engineering.TextLanguageFeature import TextLanguageFeature
from  Pipelines.Preprocess.feature_engineering.TextLengthFeature import TextLengthFeature
from  Pipelines.Preprocess.feature_engineering.TextSentimentFeature import TextSentimentFeature
from  Pipelines.Preprocess.collection.DataLoader import DataLoader
from  Pipelines.Preprocess.collection.DataLoader import DataSplit

class CONFIG:
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
        
        