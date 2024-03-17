import json
import pandas as pd
from typing import List, Tuple
from collections import Counter
from memory_profiler import profile
from functools import reduce


def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    pass


def q3_memory_json(file_path: str) -> List[Tuple[str, int]]:
    with open(file_path, "r") as file:
        mentioned_users_total = []
        for line in file:
            tweet = json.loads(line)
            if tweet["mentionedUsers"]:
                mentioned_users = [
                    user["username"]
                    for user in tweet["mentionedUsers"]
                    if len(tweet["mentionedUsers"]) > 0 
                ]
                mentioned_users_total.extend(mentioned_users)
        counter_mentioned_users = Counter(mentioned_users_total)
        top_influyent_users = counter_mentioned_users.most_common(10)
        return top_influyent_users


def q3_memory_pandas(file_path: str) -> List[Tuple[str, int]]:
    tweets = pd.read_json(
        file_path,
        lines=True,
        chunksize=1000,
        convert_dates=True,
        dtype={
            "url": "bool",
            "date": "bool",
            "content": "string",
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

    user_list = map(process_tweets, tweets)
    result = reduce(lambda x, y: x + y, user_list)
    counter_mentioned_users = Counter(result)
    top_influyent_users = counter_mentioned_users.most_common(10)
    return top_influyent_users


def process_tweets(df: pd.DataFrame):
    df = df.loc[:, ["mentionedUsers"]].reset_index()
    df = df["mentionedUsers"].apply(lambda x: get_usernames(x))
    user_extracted = [
        user
        for user_list in df.values.tolist()
        for user in user_list
        if len(user_list) > 0
    ]
    return user_extracted


def get_usernames(df: pd.DataFrame):
    if df:
        list_of_users = [user["username"] for user in df]
        return list_of_users
    return []


def add(previus_result: pd.DataFrame, new_result: pd.DataFrame):
    return pd.concat([previus_result, new_result])
