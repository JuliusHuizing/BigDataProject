from  modules.Preprocess.cleaning.DropNull import CleanDirtyRows
from  modules.Preprocess.cleaning.CleanDuplicateRows import CleanDuplicateRows

from  modules.Preprocess.feature_engineering.TextLanguageFeature import TextLanguageFeature
from  modules.Preprocess.feature_engineering.TextLengthFeature import TextLengthFeature
from  modules.Preprocess.feature_engineering.TextSentimentFeature import TextSentimentFeature
from  modules.Preprocess.collection.DataLoader import DataLoader
from  modules.Preprocess.collection.DataLoader import DataSplit

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
    
        
        