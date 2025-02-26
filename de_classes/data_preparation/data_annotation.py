## Author: Wong Yee En
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType

class DataAnnotator:
    def __init__(self, spark_session):
        self.spark = spark_session

    @staticmethod
    def annotate_sentiment(star_count):
        if star_count in [4, 5]:
            return 2
        elif star_count == 3:
            return 1
        else:
            return 0

    def add_sentiment_column(self, df):
        annotate_sentiment_udf = udf(self.annotate_sentiment, IntegerType())
        df_cleaned = df.withColumn('Sentiment', annotate_sentiment_udf(col('StarCount')))
        return df_cleaned
