import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs

def run_twitter_etl():
    # Twitter API credentials
    consumer_key = "*" 
    consumer_secret = "*"
    access_token  = "*"
    access_token_secret = "*"
    bearer_token = "*"

    # Create the authentication object
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # Create the API object while passing in auth information
    api = tweepy.API(auth)

    client = tweepy.Client(bearer_token, consumer_key, consumer_secret, access_token, access_token_secret)
    # Create a tweet list as follows:
    # tweets = api.user_timeline(screen_name='@elonmusk', count=200, include_rts = False, tweet_mode = 'extended')

    query = 'from:elonmusk -is:retweet'
    tweets = client.search_recent_tweets(query=query, max_results=100, tweet_fields='created_at,author_id,public_metrics,entities', expansions='author_id')

    list = []
    for tweet in tweets.data:
        list.append({'id':tweets.includes['users'][0].id, 'name':tweets.includes['users'][0].name, 'username':tweets.includes['users'][0].username, 'tweet':tweet.text, 'text_length':len(tweet.text)})

    df = pd.DataFrame(list)
    df.to_csv('s3://hanfiev-twitter-airflow-bucket/tweets.csv')
    