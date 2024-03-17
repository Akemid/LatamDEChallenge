import pandas as pd
from typing import List, Tuple
from collections import Counter
from memory_profiler import profile
from functools import reduce

@profile(precision=4)
def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    # Read the file in chunks and then process the chunks
    tweets = pd.read_json(
        file_path,
        lines=True,
        chunksize=1000,
        convert_dates=True,
        dtype={
            "url": "bool",
            "date": "bool",
            "content": "bool",
            "renderedContent": "bool",
            "id": "bool",
            "user": "bool",
            "outlinks": "bool",
            "tcooutlinks": "bool",
            "replyCount": "bool",
            "retweetCount": "bool",
            "likeCount": "bool",
            "quoteCount": "bool",
            "conversationId": "bool",
            "lang": "bool",
            "source": "bool",
            "sourceUrl": "bool",
            "sourceLabel": "bool",
            "media": "bool",
            "retweetedTweet": "bool",
            "quotedTweet": "bool",
            "mentionedUsers": "object",
        },
    )
    # Each chunk is processed and then added to the result
    user_list = map(process_tweets, tweets)
    # The result is reduced to a single list
    result = reduce(lambda x, y: x + y, user_list)
    # Count the users using Counter
    counter_mentioned_users = Counter(result)
    # Get the top 10 users
    top_influyent_users = counter_mentioned_users.most_common(10)
    return top_influyent_users


def process_tweets(df: pd.DataFrame):
    # Select the column mentionedUsers
    df = df.loc[:, ["mentionedUsers"]].reset_index()
    # Get the usernames from the column mentionedUsers
    df = df["mentionedUsers"].apply(lambda x: get_usernames(x))
    # Flatten the list of users
    user_extracted = [
        user
        for user_list in df.values.tolist()
        for user in user_list
        if len(user_list) > 0
    ]
    return user_extracted


def get_usernames(df: pd.DataFrame):
    # Get the username from the column mentionedUsers
    if df:
        # Get the username and avoid keys that we are not going to use
        list_of_users = [user["username"] for user in df]
        return list_of_users
    return []


def add(previus_result: pd.DataFrame, new_result: pd.DataFrame):
    return pd.concat([previus_result, new_result])
