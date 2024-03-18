# # Define configuration variables directly
# DATA_PATH = "data/"
# PREDICTIONS_PATH = "predictions/"

# use class instead of direct config variables so we get type completion functionality
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


class Config:
    DATA_PATH = "data/"
    VALIDATION_DATASET_NAME = "validation_hidden.csv"
    TEST_DATASET_NAME = "test_hidden.csv"
    
    PREDICTIONS_PATH = "predictions/"
    VALIDATION_PREDICTIONS_NAME = "validation_predictions.csv"
    TEST_PREDICTIONS_NAME = "test_predictions.csv"
    
    MODEL_PATH = "model"
    
    TRAIN_SCHEMA = [
        # although ids are not in specified as column in csv, they are there. So ignore the runtime warnings there.
        StructField("id", IntegerType(), True), 
        StructField("product_id", StringType(), True),
        StructField("product_parent", IntegerType(), True),
        StructField("product_title", StringType(), True),
        StructField("vine", StringType(), True),
        StructField("verified_purchase", StringType(), True),
        StructField("review_headline", StringType(), True),
        StructField("review_body", StringType(), True),
        StructField("review_date", StringType(), True),
        StructField("marketplace_id", IntegerType(), True),
        StructField("product_category_id", IntegerType(), True),                
        StructField("label", StringType(), True)
    ]
    VALIDATION_SCHEMA = TRAIN_SCHEMA[:-1]
    TEST_SCHEMA = VALIDATION_SCHEMA
    
