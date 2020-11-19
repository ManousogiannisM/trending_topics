from datetime import datetime
import click
from trending_topics import get_trending_topics

@click.command()
@click.argument("path", type=click.Path(exists=True))
@click.argument("timestamp", type=str)
@click.argument("n", type=int)
@click.option("--time_interval", type=str, default="1 hour", help="Interval size to check for trends.")
@click.option("--past_intervals", type=int, default=100, help="Number of past intervals to compare to.")
@click.option("--use_hashtags", default=True, type=bool, help="Use hashtags instead of tweet text.")
def main(path, timestamp, n, time_interval, past_intervals, use_hashtags):
    """
    Print the N top trending topics at TIMESTAMP (specified in ISO format), using the
    json file at PATH.

    Example:
    trending_topics ./sample.json "2011-02-25 16:30:00" 5
    """
    df = get_trending_topics(
        path=path,
        timestamp=datetime.fromisoformat(timestamp),
        n=n,
        time_interval=time_interval,
        past_intervals=past_intervals,
        use_hashtags=use_hashtags,
    )

    df.show()
