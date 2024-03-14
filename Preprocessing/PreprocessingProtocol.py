from typing import Protocol
from pyspark.sql import DataFrame

from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, IntegerType, FloatType
from langdetect import detect
from transformers import pipeline as TransformersPipeline

class ReviewLanguageAugmenter:
    def process(self, data: DataFrame) -> DataFrame:
        detectLanguageUDF = udf(lambda x: self.detect_language(x), StringType())
        return data.withColumn("review_language", detectLanguageUDF(col("review_body")))

    @staticmethod
    def detect_language(text: str) -> str:
        try:
            return detect(text)
        except Exception:
            return "unknown language"

class ReviewLengthAugmenter:
    def process(self, data: DataFrame) -> DataFrame:
        lengthUDF = udf(lambda x: len(x), IntegerType())
        return data.withColumn("review_length", lengthUDF(col("review_body")))

class HeaderLengthAugmenter:
    def process(self, data: DataFrame) -> DataFrame:
        lengthUDF = udf(lambda x: len(x) if x is not None else 0, IntegerType())
        return data.withColumn("header_length", lengthUDF(col("review_headline")))

class ReviewSentimentAugmenter:
    def __init__(self):
        self.distilled_student_sentiment_classifier = TransformersPipeline(
            model="lxyuan/distilbert-base-multilingual-cased-sentiments-student",
            return_all_scores=False
        )

    def process(self, data: DataFrame) -> DataFrame:
        sentimentAnalyzer = udf(lambda x: self.compute_sentiment(x), FloatType())
        return data.withColumn("review_sentiment", sentimentAnalyzer(col("review_body")))

    def compute_sentiment(self, text: str) -> float:
        if text is None or len(text) == 0:
            return 0.0
        sentiment_list = self.distilled_student_sentiment_classifier([text])
        sentiment_dict = sentiment_list[0]
        sign = 1 if sentiment_dict["label"] == "positive" else -1
        return sentiment_dict["score"] * sign



