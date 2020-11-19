from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from trending_topics.topics_preprocessing import topics_lowercase,preprocess_topics


def filter_null(df: DataFrame) -> DataFrame:
    "Filter out entries where the timestamp or text are null"
    return df.where(~(F.isnull("created_at") | F.isnull("text")))


def parse_timestamps(df: DataFrame):
    "Add a `timestamp` and `unix_timestamp` column."
    return (
        df
        .withColumn(
            "timestamp",
            F.to_timestamp("created_at", "EEE MMM dd HH:mm:ss Z yyyy").alias("timestamp")
        )
        #.withColumn("unix_timestamp", F.unix_timestamp("timestamp"))
    )


def get_topics(df: DataFrame, hashtags=True):
    "Gather tweet text or hashtags and make each row represent 1 topic"
    if hashtags:
        topics = df.withColumn("raw_topic", F.explode(F.expr("entities.hashtags.text")))
        topics = topics_lowercase(topics)
    else:
        topics = df.withColumnRenamed('text','raw_topic')
        topics = preprocess_topics(topics)
        # topics = (
        #     df
        #     .withColumn("split_text", F.split("text", r"\s+"))
        #     .withColumn("topic", F.explode("split_text"))
        #     .drop("split_text")
        # )

    return topics


def count_within_timeframe(df: DataFrame, timeframe: str = "30 minutes") -> DataFrame:
    "Add a `count` column, counting topic frequencies in intervals of size `timeframe`."
    return (
        df
        .groupBy(F.window("timestamp", timeframe), "topic")
        .agg(F.count("*").alias("count"))
    )


def calculate_slope(
    df: DataFrame,timestamp_start: datetime,timestamp_end: datetime, past_intervals: int
    = 10
) -> DataFrame:
    "Calculate the slope of the count by comparing the count to the past average."

    window = (
        Window
        .partitionBy("topic")
        .orderBy(F.expr("window.start"))
        .rowsBetween(Window.currentRow - past_intervals, Window.currentRow - 1)
    )
    return (
        df
        .withColumn("past_mean", F.mean("count").over(window))
        .withColumn("slope", (F.col("count") - F.col("past_mean")) / F.col("past_mean"))
        .drop("past_mean")
        .where((timestamp_start <= F.expr("window.start")) & (timestamp_end >= F.expr("window.end")))
    )


# def trending_topics(df: DataFrame, timestamp: datetime, n: int = 5) -> DataFrame:
#     "Return the `n` topics with the biggest slope at the given timestamp."
#     window = Window.orderBy(F.desc("slope"))
#     return (
#         df
#         .where((timestamp >= F.expr("window.start")) & (timestamp < F.expr("window.end")))
#         .withColumn(
#             "trend_rank", F.rank().over(window)
#         )
#         .where(F.col("trend_rank") <= n)
#     )


def trending_topics(df: DataFrame, n: int = 5, instant: bool=False) -> DataFrame:
    if instant:
        window = Window.orderBy(F.desc("slope"))
        return (
            df
            .withColumn(
                "trend_rank", F.rank().over(window)
            )
            .where(F.col("trend_rank") <= n)
        )
    else:
        return (
            df.groupBy('topic')
            .agg(F.avg(F.col('slope')).alias('average_slope'))
            .orderBy(F.desc('average_slope'))
            .limit(n)
        )
