import ijson
import json
import io
import pandas as pd
import polars as pl
from typing import List, Tuple
from datetime import datetime
from collections import defaultdict
from memory_profiler import profile
from functools import reduce
import sys


@profile(precision=4)
def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    # The selected function is the one that has the best performance,
    # It is the one that uses pandas with the best memory optimization.
    # The line by line function is the one that uses the least memory but it is the slowest
    # And for that reason it is not the best option.
    return q1_memory_pandas(file_path=file_path)

@profile(precision=4)
def q1_memory_line_by_line(file_path: str) -> List[Tuple[datetime.date, str]]:
    # We need to optimize the memory
    # Read line by line using json
    tweets_dict = defaultdict(lambda: defaultdict(int))
    with open(file_path, 'r', encoding='utf-8') as file:
        # Read line by line
        for line in file:
            # Load the json
            tweet = json.loads(line)
            # Extract the date and convert into date 
            date = datetime.strptime(tweet['date'], "%Y-%m-%dT%H:%M:%S%z").date()
            # Extract the username
            username = tweet['user']['username']
            # Count by date and username
            tweets_dict[date][username] += 1
    # Get all the data in form of item (date, {user:count})
    tweet_dict_items = tweets_dict.items()
    # Sort the dates by the sum of the tweets counts of each user and order desc
    tweets_dict = sorted(tweet_dict_items, key=lambda x:sum(x[1].values()), reverse=True)
    # Get the top 10 dates
    tweets_dict = tweets_dict[:10]
    # Get in dict format
    tweets_dict = dict(tweets_dict)
    # Creating a list with comprehension in order to save memory
    users_top_tweets = [
                        # Get the user with the most tweets for each date
                        max(tweets_dict[date],key=lambda x: tweets_dict[date][x])
                        for date in tweets_dict.keys()
                        ]        
    return list(zip(tweets_dict.keys(), users_top_tweets))
   

@profile(precision=4)
def q1_memory_pandas(file_path: str,chunksize=1000) -> List[Tuple[datetime.date, str]]:
    # Many options to optimize the memory
    # Chunksize allows to read the file in chunks
    # Dtypes allows to specify the type of the columns and loss information that is not needed
    # in order to save memory
    tweets = pd.read_json(file_path, lines=True,chunksize=chunksize,convert_dates=True,dtype={
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
    procced_tweets = map(process_tweets, tweets)
    # The result is reduced to a single dataframe
    result = reduce(add, procced_tweets)
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

@profile(precision=4)
def q1_memory_polars(file_path: str) -> List[Tuple[datetime.date, str]]:
    query = read_lazy_json(file_path)
    result = query.collect()
    return result.rows()

@profile(precision=4) 
def read_lazy_json(file_path:str):
    tweets = pl.scan_ndjson(file_path,low_memory=True)
    tweets = tweets.select(
        [pl.col("date").cast(pl.Datetime).cast(pl.Date), pl.col("user").struct.field("username").alias("user")]
    )
    tweets = tweets.group_by(["date","user"]).agg(
          pl.col("date").count().alias("count"),
        )    
    tweets = tweets.sort(
        by=["date","count"],descending=True
        ).group_by("date").agg(
            pl.col("date").count().alias("count"),
            pl.col("user").first().alias("user")
        ).sort(
            by="count",descending=True
        ).limit(10).select(
            pl.col("date"),
            pl.col("user")
        )
    return tweets


if __name__ == "__main__":
    file_path = 'datasets/farmers-protest-tweets-2021-2-4.json'
    #Read args to console
    if len(sys.argv) < 2:
        result = q1_memory('datasets/farmers-protest-tweets-2021-2-4.json')
    else:    
        # Execute different functions based on the argument
        if sys.argv[1] == "line_by_line":
            result = q1_memory_line_by_line(file_path)
        elif sys.argv[1] == "polars":
            result = q1_memory_polars(file_path)
        elif sys.argv[1] == "pandas":
            result = q1_memory_pandas(file_path)
        else:
            print("Invalid argument.")
            sys.exit(1)
    print(result)
    
    