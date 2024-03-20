from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import FloatType
from transformers import pipeline as TransformersPipeline

class TextSentimentFeature:
    def __init__(self, input_column_name: str = "review_body", output_column_name: str = "review_sentiment"):
        """
        Initializes the feature engineering module to compute the sentiment score of the text in a specified column using a sentiment analysis model.

        Parameters:
        - input_column_name: The name of the column containing the text to analyze. Defaults to "review_body".
        - output_column_name: The name of the output column where the sentiment score will be stored. Defaults to "review_sentiment".
        """
        self.input_column_name = input_column_name
        self.output_column_name = output_column_name
        self.distilled_student_sentiment_classifier = TransformersPipeline(
            model="lxyuan/distilbert-base-multilingual-cased-sentiments-student",
            return_all_scores=False
        )

    def process(self, data: DataFrame) -> DataFrame:
        """
        Processes the input DataFrame to compute the sentiment score of text in the specified column.

        Parameters:
        - data: A PySpark DataFrame containing the text data to analyze.

        Returns:
        - A PySpark DataFrame with an additional column for the sentiment score.
        """
        sentimentAnalyzer = udf(lambda x: self._compute_sentiment(x) if len(x) < 512 else 0, FloatType())
        return data.withColumn(self.output_column_name, sentimentAnalyzer(col(self.input_column_name)))
    
    def _compute_sentiment(self, text: str) -> float:
        """
        Computes the sentiment score of a given piece of text.

        Parameters:
        - text: The text string to analyze.

        Returns:
        - The sentiment score as a float, with positive values for positive sentiment and negative values for negative sentiment, or 0.0 for neutral or empty texts.
        """
        if text is None or len(text) == 0:
            return 0.0
        sentiment_list = self.distilled_student_sentiment_classifier([text])
        sentiment_dict = sentiment_list[0]
        sign = 1 if sentiment_dict["label"] == "positive" else -1
        return sentiment_dict["score"] * sign
