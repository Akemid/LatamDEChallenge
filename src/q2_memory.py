import emoji
from typing import List, Tuple
from collections import Counter
import pandas as pd
from memory_profiler import profile
from functools import reduce

@profile(precision=4)
def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    # The selected function is the one that has the best performance,
    # Read the file in chunks and then process the chunks
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

    # Each chunk is processed and then added to the result
    emoji_list = map(get_emojis, tweets)
    # The result is reduced to a single list
    result = reduce(lambda x,y: x+y, emoji_list)
    # Count the emojis using Counter
    counter_emojis = Counter(result)
    # Get the top 10 emojis
    top_emojis = counter_emojis.most_common(10)
    return top_emojis


def get_emojis(df: pd.DataFrame):
    # Get the emojis from the content
    emoji_list_extracted = df["content"].apply(lambda x: emoji.emoji_list(x))
    # Flatten the list of emojis
    emoji_list_extracted = [
            item["emoji"]
            for emoji_list in emoji_list_extracted.values.tolist()
            for item in emoji_list
            if len(emoji_list) > 0
        ]
    return emoji_list_extracted


