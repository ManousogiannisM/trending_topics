import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import trending_topics as tt

if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Calculate trending topics')
    parser.add_argument('--files', type=str, nargs="+", help="Files to read")
    parser.add_argument('--start_date', type=str, default="2011-02-25 22:34:50",
                        help='start date fro which trending topics will be calculated: yyyy-mm-dd hh:mm:ss')
    parser.add_argument('--end_date', type=str, default="2011-02-26 22:34:50",
                        help='end date fro which trending topics will be calculated: yyyy-mm-dd hh:mm:ss')
    parser.add_argument('--minimum_time_interval', type=str, default="1 hour",
                        help='minimum time interval in which a slope is calculated')
    parser.add_argument('--number_of_trends', type=int, default=5,
                        help='number of top trends to be returned')
    parser.add_argument('--instant_trends', type=bool, default=False,
                        help='Whether to calculate instant or total trends within the given time interval')
    args = parser.parse_args()

    # new_schema = StructType.fromJson(json.loads(df.schema.json()))
    file_paths = args.files
    min_interval = args.minimum_time_interval

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    df = spark.read.json(file_paths[0])
    for path in file_paths[1:]:
        df = df.union(spark.read.json(path))

    df = tt.filter_null(df)
    df = tt.parse_timestamps(df)
    df = tt.get_topics(df, hashtags=True)
    df = df.select("timestamp", "topic")
    df = tt.count_within_timeframe(df).orderBy(F.desc("count"))
    df = tt.calculate_slope(
        df,
        datetime.fromisoformat(args.start_date),
        datetime.fromisoformat(args.end_date),
    )

    tt.trending_topics(df, args.number_of_trends, args.instant_trends).show()
