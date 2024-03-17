import emoji
from typing import List, Tuple
from collections import Counter
import pandas as pd
from memory_profiler import profile
from functools import reduce

@profile(precision=4)
def q2_memory(file_path: str) -> List[Tuple[str, int]]:
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
            "mentionedUsers": "bool",
        },
    )

    emoji_list = map(get_emojis, tweets)
    result = reduce(lambda x,y: x+y, emoji_list)
    counter_emojis = Counter(result)
    top_emojis = counter_emojis.most_common(10)
    return top_emojis


def get_emojis(df: pd.DataFrame):
    emoji_list_extracted = df["content"].apply(lambda x: emoji.emoji_list(x))
    emoji_list_extracted = [
            item["emoji"]
            for emoji_list in emoji_list_extracted.values.tolist()
            for item in emoji_list
            if len(emoji_list) > 0
        ]
    return emoji_list_extracted

if __name__ == "__main__":
    file_path = "datasets/farmers-protest-tweets-2021-2-4.json"
    q2_memory(file_path)
