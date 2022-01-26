import redis
from tqdm import tqdm
import pandas as pd
import tweepy
import time

def load_tweet_ids():
    chunksize = 10 ** 6

    LINES = 332706325

    frames = []

    iter = 0
    with tqdm(total = LINES) as pbar:
        for chunk in pd.read_csv('full_dataset_clean.tsv', chunksize=chunksize, delimiter='\t'):
            frames.append(chunk[chunk["lang"] == "pl"])

            pbar.update(len(chunk))

    result = pd.concat(frames)
    result = result.reset_index()
    
    print(result.head())
    
    result.to_csv('full_dataset_clean_pl.csv')

    return result

def download(ids: pd.DataFrame):

    api_key = "wih5UnfzJdrDqbDmT8s8Q1eOb"
    api_key_secret = "XzX24SMSa5uEuqpOuPq9kOKmEmKncifbOzERmeBgsIwf434Lq2"
    bearer_token = "AAAAAAAAAAAAAAAAAAAAAMSBYQEAAAAAc8cHzyylG4bieF6aiobvH7OhNAo%3DIkidGH9zpBZhEwwPQS1DxZVk2gJnVx4YYBaykWEbuS01T7066e"

    client = tweepy.Client(bearer_token)

    r = redis.Redis(
        host='127.0.0.1',
        port=6379, 
        password='Kr7Wygxq3y')


    for i in tqdm(range(len(ids))):
        tweet_id = str(ids.iloc[i].tweet_id)

        if r.get(tweet_id) is None:
            try:
                response = client.get_tweet(tweet_id)
                time.sleep(1.01)
            except tweepy.TooManyRequests as tmr:
                time.sleep(1 * 60)
            if response.errors:
                r.set(tweet_id, "")
            else:
                r.set(tweet_id, str(response.data))

tweet_ids = pd.read_csv('full_dataset_clean_pl.csv')
download(tweet_ids)

