
import polars as pl
from typing import List, Tuple
from collections import Counter

def q3_time(file_path: str) -> List[Tuple[str, int]]:   
    # Read the file json
    query = read_lazy_json(file_path)
    # Begin to execute all the operations previously defined
    result = query.collect()
    # Get the list of user from column users
    mentioned_users_total = result.get_column("users").to_list()
    # Flatten the list of users
    mentioned_users_total = [user for user_row in mentioned_users_total for user in user_row]
    # Count the users using Counter
    counter_mentioned_users = Counter(mentioned_users_total)
    # Get the top 10 users
    top_influyent_users = counter_mentioned_users.most_common(10)
    return top_influyent_users

def read_lazy_json(file_path: str):
    # Read the file json with API lazy
    tweets = pl.scan_ndjson(
        file_path,
        low_memory=True,
    )
    # Select the columns that we are going to use
    # mentionedUsers
    tweets = tweets.select(
        pl.col("mentionedUsers").map_batches(
            lambda s: map_emojis(s),
        ).alias("users")
    ).filter(
        # Filter the rows that doesn't have users
        pl.col("users") != []
    )
    return tweets

def map_emojis(df:pl.DataFrame):
    def get_usernames(x:pl.DataFrame|None):
        # Check if the value is a pl.DataFrame
        if isinstance(x,pl.DataFrame):
            ## Get the username and avoid keys that we are not going to use
            return [user["username"] for user in x]
        return []
    # Map to get the list of usernames
    f_mapped = map(get_usernames,df)
    return pl.Series(list(f_mapped))