import emoji
import polars as pl
from typing import List, Tuple
from collections import Counter

def q2_time(file_path: str) -> List[Tuple[str, int]]:    
    # Read the file json
    query = read_lazy_json(file_path)
    # Begin to execute all the operations previously defined
    result = query.collect()
    # Get the list of emojis from column emojis
    emoji_list = result.get_column("emojis").to_list()
    # Flatten the list of emojis
    emoji_list = [emoji for emoji_row in emoji_list for emoji in emoji_row]
    # Count the emojis using Counter
    counter_emojis = Counter(emoji_list)
    # Get the top 10 emojis
    top_emojis = counter_emojis.most_common(10)
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
            lambda s: map_emojis(s),
        ).alias("emojis")
    ).filter(
        # Filter the rows that doesn't have emojis
        pl.col("emojis") != []
    )
    return tweets

def map_emojis(df):
    def get_just_emoji(x):
        # Get the emojis and avoid keys that we are not going to use
        return [element["emoji"] for element in emoji.emoji_list(x)]
    # Map the emojis 
    f_mapped = map(lambda x: get_just_emoji(x),df)
    return pl.Series(list(f_mapped))


