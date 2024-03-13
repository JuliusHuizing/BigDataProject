import pandas as pd
import os
from enum import Enum
from DataSplit import DataSplit, DataSet

import logging
# os.environ['PYSPARK_SUBMIT_ARGS'] = "--master mymaster --total-executor 2 --conf 'spark.driver.extraJavaOptions=-Dhttp.proxyHost=proxy.mycorp.com-Dhttp.proxyPort=1234' -Dhttp.nonProxyHosts=localhost|.mycorp.com|127.0.0.1 -Dhttps.proxyHost=proxy.mycorp.com -Dhttps.proxyPort=1234 -Dhttps.nonProxyHosts=localhost|.mycorp.com|127.0.0.1 pyspark-shell"
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnan, when, count
from pyspark.ml.feature import Imputer, StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import os
from langdetect import detect
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, IntegerType, FloatType


from transformers import pipeline as TransformersPipeline

class DataAugmenter:  
    @staticmethod  
    def augment_data(data: DataSet) -> DataSet:
        data = DataAugmenter.augment_with_review_language(data)
        data = DataAugmenter.augment_with_review_length(data)
        data = DataAugmenter.augment_with_header_length(data)
        # data = DataAugmenter.augment_with_review_sentiment(data)
        return data

    @staticmethod 
    def augment_with_review_language(data: DataSet) -> DataSet:
        def detect_language(text: str):
            try:
                detected_language = detect(text)
                return detected_language
            except Exception as e:  
                # print("An error occurred:", e)
                return "unkown language"
            
        detectLanguageUDF = udf(lambda x: detect_language (x),StringType())
        data.df = data.df.withColumn("review_language", detectLanguageUDF(col("review_body")))
        return data
    
    @staticmethod 
    def augment_with_review_length(data: DataSet) -> DataSet:
        lengthUDF = udf(lambda x: len(x), IntegerType())
        data.df = data.df.withColumn("review_length", lengthUDF(col("review_body")))
        return data

    @staticmethod 
    def augment_with_header_length(data: DataSet) -> DataSet:
        lengthUDF = udf(lambda x: len(x) if x != None else 0, IntegerType())
        data.df = data.df.withColumn("header_length", lengthUDF(col("review_headline")))
        return data
    
    @staticmethod
    def augment_with_review_sentiment(data: DataSet) -> DataSet:
        distilled_student_sentiment_classifier = TransformersPipeline(
        model="lxyuan/distilbert-base-multilingual-cased-sentiments-student", 
        return_all_scores=False
        )
        # this model gives back a dict with the sentiment and the score
        # because we want a simple datatype, we transform the output to always be a float (-1 being most negative, +1 being most positive, 0 neutral)
        def compute_sentiment(text) -> float:
            if text == None:
                return 0
            if len(text) == 0:
                return 0
            
            sentiment_list = distilled_student_sentiment_classifier([text])
            sentiment_dict = sentiment_list[0]
            sign = 1 if sentiment_dict["label"] == "positive" else -1
            return sentiment_dict["score"] * sign
            
        sentiment_analyer = udf(lambda x: compute_sentiment(x), FloatType())

        data.df = data.df.withColumn("review_sentiment", sentiment_analyer(col("review_body")))
        return data

        
    
    
  
    
    

        

        
            
            

            