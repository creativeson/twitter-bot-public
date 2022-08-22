import tweepy
import time
from dotenv import load_dotenv
import os
load_dotenv()

API_KEY = os.getenv("api_key")
API_KEY_SECRET = os.getenv("api_key_secret")
ACCESS_TOKEN = os.getenv("access_token")
ACCESS_TOKEN_SECRET =os.getenv("access_token_secret")

auth = tweepy.OAuthHandler(API_KEY, API_KEY_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth, wait_on_rate_limit=True)

FILE_NAME = 'last_seen_id.txt'


# read file that stored seen id
def get_seen_id(file_name):
    f_read = open(file_name, 'r')
    last_seen_id = int(f_read.read().strip())
    f_read.close()
    return last_seen_id

# read file that stored seen id
def store_seen_id(last_seen_id, file_name):
    f_write = open(file_name, 'w')
    f_write.write(str(last_seen_id))
    f_write.close()
    return

# reply the comment that include #hola
def reply(second):
    while True:
        print('retrieving and replying to tweets...', flush=True)
        last_seen_id = get_seen_id(FILE_NAME)

        mentions = api.mentions_timeline(sinceid=last_seen_id)

        for mention in reversed(mentions):
            print(str(mention.id) + ' - ' + mention.text, flush=True)

            store_seen_id(last_seen_id, FILE_NAME)
            reply_to = mention.user.screen_name

            if '#hola' in mention.text.lower():
                print('found #hola', flush=True)
                print('responding back', flush=True)
                message = '@' + reply_to + ' Nice to meet you!'
                try:
                    api.update_status(message,in_reply_to_status_id = last_seen_id)
                except Exception as e:
                    print(e)
                    pass
        time.sleep(second)