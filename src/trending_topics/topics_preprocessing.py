from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.ml.feature import Tokenizer, StopWordsRemover


def topics_lowercase(df:DataFrame,output_col_name:str='topic',input_col_name:str='raw_topic') -> DataFrame:
    """lowercases the topic cloumn of the dataframe"""
    return df.withColumn(output_col_name, F.lower(input_col_name))


def preprocess_topics(df:DataFrame,output_col_name:str='topic',input_col_name:str='raw_topic') -> DataFrame:
    """1. tokenizes the input text column 2. removes stopwords
    3. creates a new column with postprocessed topics"""

    df = topics_tokenize(df,'tokenized_topics',input_col_name)
    df = remove_stopwords(df,'no_stopwords_topics','tokenized_topics')

    return (df
            .withColumn(output_col_name,F.explode('no_stopwords_topics'))
            )


def topics_tokenize(df:DataFrame,output_col_name:str='topic',input_col_name:str='raw_topic') -> DataFrame:
    """tokenizes a multi-token topic column"""
    tokenizer = Tokenizer(inputCol=input_col_name, outputCol=output_col_name)
    # alternatively, pattern="\\w+", gaps(False)

    tokenized_df = tokenizer.transform(df)
    return tokenized_df


def remove_stopwords(df:DataFrame,output_col_name:str='topic',input_col_name:str='raw_topic') -> DataFrame:
    "removes all stopwords from topic column"
    remover = StopWordsRemover(inputCol=input_col_name, outputCol=output_col_name)
    return remover.transform(df)

# class TopicDataframePreprocessor:
#
#     def __init__(self,df: DataFrame,input_topic_col: str, output_topic_col:str='topics'):
#         self._df=df
#         self._input_topic_col=input_topic_col
#         self._output_topic_col=output_topic_col
#
#     def topics_lowercase(self)-> DataFrame:
#         """lowercases the topic cloumn of the dataframe"""
#         return self._df.withColumn(self._output_topic_col, F.lower(self._input_topic_col))
#
#     def topics_tokenize(self)-> DataFrame:
#         """tokenizes a multi-token topic column"""
#         tokenizer = Tokenizer(inputCol=self._input_topic_col, outputCol=self._output_topic_col)
#     # alternatively, pattern="\\w+", gaps(False)
#
#         tokenized_df = tokenizer.transform(self._df)
#         return tokenized_df
#
#     def remove_stopwords(self)-> DataFrame:
#         "removes all stopwords from topic column"
#         remover = StopWordsRemover(inputCol=self._input_topic_col, outputCol=self._output_topic_col)
#         return remover.transform(self._df)
#
