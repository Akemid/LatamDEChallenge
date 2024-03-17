import json
import emoji
import polars as pl
from typing import List, Tuple
from collections import Counter


def q2_time(file_path: str) -> List[Tuple[str, int]]:
    pass


def q2_time_json(file_path: str) -> List[Tuple[str, int]]:
    with open(file_path, "r") as f:
        emoji_list_total = []
        for line in f.readlines():
            tweet = json.loads(line)
            emoji_list_extracted = emoji.emoji_list(tweet["content"])
            emoji_list_extracted = [emoji["emoji"] for emoji in emoji_list_extracted]
            emoji_list_total.extend(emoji_list_extracted)
        counter_emojis = Counter(emoji_list_total)
        top_emojis = counter_emojis.most_common(10)
        return top_emojis


def q2_time_polars(file_path: str) -> List[Tuple[str, int]]:
    # Read the file json
    query = read_lazy_json(file_path)
    # Begin to execute all the operations previously defined
    result = query.collect()
    emoji_list = result.get_column("emojis").to_list()
    emoji_list = [emoji for emoji_row in emoji_list for emoji in emoji_row]
    counter_emojis = Counter(emoji_list)
    top_emojis = counter_emojis.most_common(10)
    # Return the result, rows are lists of tuples 
    # List[Tuple[datetime.date, str]]
    return top_emojis

def read_lazy_json(file_path: str):
    # Read the file json with API lazy
    tweets = pl.scan_ndjson(
        file_path,
        low_memory=True,
    )
    # Select the columns that we are going to use
    # Content
    tweets = tweets.select(
        pl.col("content").map_batches(
            lambda s: function_to_map(s),
        ).alias("emojis")
    ).filter(
        pl.col("emojis") != []
    )
    return tweets

def function_to_map(df):
    def get_just_emoji(x):
        return [element["emoji"] for element in emoji.emoji_list(x)]
    f_mapped = map(lambda x: get_just_emoji(x),df)
    return pl.Series(list(f_mapped))

def get_emojis(df):
    emoji_list_extracted = df["content"].apply(lambda x: emoji.emoji_list(x))
    emoji_list_extracted = [
        item["emoji"]
        for emoji_list in emoji_list_extracted.values.tolist()
        for item in emoji_list
        if len(emoji_list) > 0
    ]
    return emoji_list_extracted

    # (

    # Select the first user for each date (the one with the most tweets)
    # .agg(
    #     pl.col("date").count().alias("count"), pl.col("user").first().alias("user")
    # )
    # # Sort by count descending all the tweets by date
    # .sort(by="count", descending=True)
    # # Select the top 10
    # .limit(10)
    # # Select the date and the user
    # .select(pl.col("date"), pl.col("user"))
    # )
