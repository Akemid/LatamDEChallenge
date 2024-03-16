import polars as pl
from datetime import datetime
from typing import List, Tuple
from datetime import datetime


def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    # Read the file json
    query = read_lazy_json(file_path)
    # Begin to execute all the operations previously defined
    result = query.collect()
    # Return the result, rows are lists of tuples with the date and the user
    # List[Tuple[datetime.date, str]]
    return result.rows()

def read_lazy_json(file_path: str):    
    # Read the file json with API lazy
    tweets = pl.scan_ndjson(
        file_path,
        low_memory=True,
    )
    # Select the columns that we are going to use
    # Date is casted to Date type
    # Username is named as user
    tweets = tweets.select(
        [
            pl.col("date").cast(pl.Datetime).cast(pl.Date),
            pl.col("user").struct.field("username").alias("user"),
        ]
    )
    # Group by date and user to count the tweets
    tweets = tweets.group_by(["date", "user"]).agg(
        pl.col("date").count().alias("count"),
    )
    tweets = (
        # Sort by date and count descending
        tweets.sort(by=["date", "count"], descending=True)
        .group_by("date")
        # Select the first user for each date (the one with the most tweets)
        .agg(
            pl.col("date").count().alias("count"), pl.col("user").first().alias("user")
        )
        # Sort by count descending all the tweets by date
        .sort(by="count", descending=True)
        # Select the top 10
        .limit(10)
        # Select the date and the user
        .select(pl.col("date"), pl.col("user"))
    )
    return tweets
