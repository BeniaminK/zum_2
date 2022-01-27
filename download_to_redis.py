import redis
import json
from tqdm import tqdm
import pandas as pd
import tweepy
import time
from typing import List, Dict
import concurrent.futures
import multiprocessing
import threading

api_key = 'wih5UnfzJdrDqbDmT8s8Q1eOb'
api_key_secret = 'XzX24SMSa5uEuqpOuPq9kOKmEmKncifbOzERmeBgsIwf434Lq2'
bearer_token = 'AAAAAAAAAAAAAAAAAAAAAMSBYQEAAAAAc8cHzyylG4bieF6aiobvH7OhNAo' \
               '%3DIkidGH9zpBZhEwwPQS1DxZVk2gJnVx4YYBaykWEbuS01T7066e '
access_token = '1459943161258811392-oi2WpXVzUwAEwn7ffwJIkzEB4lXj8z'
access_token_secret = 'DHTkA1bVDG4lVXzC7o1AqEeayq3nhHSU6AxF6Qr59F8ZC'

REDIS_BATCH = 10**6


def filter_all_tweets():
    chunksize = 10 ** 6

    LINES = 332706325

    frames = []

    print("DOING")

    with tqdm(total=LINES) as pbar:
        for chunk in pd.read_csv('full_dataset_clean.tsv', chunksize=chunksize, delimiter='\t'):
            frames.append(chunk[chunk["lang"] == "pl"])

            pbar.update(len(chunk))

    result = pd.concat(frames)
    result = result.reset_index()
    
    print(result.head())
    
    result.to_csv('full_dataset_clean_pl.csv')

    return result


def gen_batch(arr: List, size: int):
    idx_start = 0
    while True:
        finish_idx = idx_start + size
        yield arr[idx_start:finish_idx]
        idx_start = finish_idx
        if idx_start >= len(arr):
            break


def download_apiv1_1(ids: List[str], max_split=100):
    semaphore.acquire()
    auth = tweepy.AppAuthHandler(api_key,
                                 api_key_secret)

    client = tweepy.API(auth)

    texts = {}

    for ids_split in gen_batch(ids, max_split):
        while True:
            try:
                statuses = client.lookup_statuses(ids_split, map=True, trim_user=True)
            except Exception as e:
                print("v1 - exception", e)
                time.sleep(1 * 60)
                continue
            break

        for status in statuses:
            if hasattr(status, 'text'):
                texts[status.id] = status.text
            else:
                texts[status.id] = ''
    semaphore.release()
    return texts


def download_apiv2(ids: List[str], max_split=100):
    semaphore.acquire()
    client = tweepy.Client(bearer_token)

    texts = {}

    for ids_split in gen_batch(ids, max_split):
        while True:
            try:
                response = client.get_tweets(ids_split)
            except Exception as e:
                print("v2 - exception", e)
                time.sleep(1 * 60)
                continue
            break

        for tweet in response.data:
            texts[tweet.id] = tweet.text
        for error in response.errors:
            if isinstance(error, dict):
                title = error['title']
            else:
                title = error.title

            if title in {'Not Found Error', 'Authorization Error'}:
                texts[error['resource_id']] = ''
    semaphore.release()
    return texts


processes = [download_apiv1_1, download_apiv2]


def save_tweets(redis_connector: redis.Redis, tweets_dict: Dict[str, str]):

    inserted_ctr = 0

    for tweet_id, tweet_text in tweets_dict.items():
        assert redis_connector.get(tweet_id) is None

        ret = redis_connector.set(tweet_id, tweet_text)

        if ret:
            inserted_ctr += 1

    return inserted_ctr


def redis_to_file():
    dct = {}
    tweet_ids = list(r.scan_iter(count=REDIS_BATCH))
    tweet_values = list(r.mget(tweet_ids))
    print("Loading from redis to memory...")
    for tweet_id, tweet_value in tqdm(zip(tweet_ids, tweet_values), total=len(tweet_values)):
        dct[tweet_id.decode('utf-8')] = tweet_value.decode('utf-8')

    print("Saving...")
    with open('redis.json', 'w', encoding='utf-8') as f:
        json.dump(dct, f, indent=2)
    print(f"Stored {len(dct)} tweets.")


if __name__ == '__main__':

    all_tweet_ids = set(pd.read_csv('full_dataset_clean_pl.csv').tweet_id.map(str).tolist())

    r = redis.Redis(
        host='127.0.0.1',
        port=6379,
        password='Kr7Wygxq3y')

    saved_tweets = set(map(lambda x: x.decode('utf-8'), r.scan_iter(count=REDIS_BATCH)))

    tweet_ids_to_dl = list(set.difference(all_tweet_ids, saved_tweets))

    already_saved_ctr = len(saved_tweets)

    batch_generator = gen_batch(tweet_ids_to_dl, 100)

    pbar = tqdm(total=len(all_tweet_ids))
    pbar.update(already_saved_ctr)

    semaphore = threading.Semaphore(len(processes))

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(processes))

    futures: List[concurrent.futures.Future] = [None for process in processes]

    for batch in gen_batch(tweet_ids_to_dl, 100):

        for idx in range(len(processes)):

            future = futures[idx]
            process = processes[idx]

            if future is None:
                futures[idx] = executor.submit(process, batch)

                break
            elif future.done():

                # save future result
                tweets = future.result()
                saved = save_tweets(r, tweets)
                assert saved == len(tweets)
                already_saved_ctr += saved
                pbar.update(saved)

                # create new future
                futures[idx] = executor.submit(process, batch)

                break

        semaphore.acquire(blocking=True)
        semaphore.release()

    pbar.close()

    redis_to_file()
