import pandas as pd
from typing import List, Tuple
from datetime import datetime
from memory_profiler import profile
from functools import reduce


@profile(precision=4)
def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    # The selected function is the one that has the best performance,
    # It is the one that uses pandas with the best memory optimization.
    # The line by line function is the one that uses the least memory but it is the slowest
    # And for that reason it is not the best option.
    
    # OPTIMIZATION:
    # Many options to optimize the memory
    # Chunksize allows to read the file in chunks
    # Dtypes allows to specify the type of the columns and loss information that is not needed
    # in order to save memory
    tweets = pd.read_json(file_path, lines=True,chunksize=1000,convert_dates=True,dtype={
        "url":"bool",
        "date":"datetime64[ns]",
        "content":"bool",
        "renderedContent": "bool",
        "id": "bool",
        "user":"object",
        "outlinks":"bool",
        "tcooutlinks":"bool",
        "replyCount":"bool",
        "retweetCount":"bool",
        "likeCount":"bool",
        "quoteCount":"bool",
        "conversationId":"bool",
        "lang":"bool",
        "source":"bool",
        "sourceUrl":"bool",
        "sourceLabel":"bool",
        "media":"bool",
        "retweetedTweet":"bool",
        "quotedTweet":"bool",
        "mentionedUsers":"bool",
    })
    # Each chunk is processed and then added to the result
    processed_tweets = map(process_tweets, tweets)
    # The result is reduced to a single dataframe
    result = reduce(add, processed_tweets)
    # The top dates are extracted
    top_dates = result.groupby(['date']).sum().sort_values(ascending=False).head(10).index.values.tolist()
    users = []
    result = result.reset_index()   
    # The top user for each date is extracted 
    for date in top_dates:
        # Filter by date
        top_twitter_user = result[result['date'] == date][["user",0]]
        # Get the sum of the tweets for each user in the date
        top_twitter_user = top_twitter_user.groupby(['user']).sum()    
        # Get the user with the most tweets
        top_twitter_user = top_twitter_user.sort_values(by=0,ascending=False).head(1)
        # Get the username
        top_twitter_user = top_twitter_user.index.values.tolist()[0]
        users.append(top_twitter_user)
    return list(zip(top_dates, users)) 

 
def process_tweets(df_tweets:pd.DataFrame):    
    df_tweets['date'] = df_tweets['date'].dt.date
    df_tweets = df_tweets.loc[:,["date","user"]].reset_index()
    df_tweets["user"] = pd.json_normalize(df_tweets["user"])["username"]
    df_tweets = df_tweets.groupby(['date','user']).size()  
    return df_tweets

def add(previus_result:pd.DataFrame, new_result:pd.DataFrame):
    return pd.concat([previus_result,new_result])

    
    