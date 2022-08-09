import tweepy
import time

api_key = 'AR92E'
api_key_secret = 'snwd'
access_token = 'ZfyK'
access_token_secret = 'TCF'

auth = tweepy.OAuthHandler(api_key, api_key_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)

FILE_NAME = 'last_seen_id.txt'

def get_seen_id(file_name):
    f_read = open(file_name, 'r')
    last_seen_id = int(f_read.read().strip())
    f_read.close()
    return last_seen_id

def store_seen_id(last_seen_id, file_name):
    f_write = open(file_name, 'w')
    f_write.write(str(last_seen_id))
    f_write.close()
    return

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
                print('responding back...', flush=True)
                message = '@' + reply_to + ' Nice to meet you!'
                try:
                    api.update_status(message,in_reply_to_status_id = last_seen_id)
                except Exception as e:
                    print(e)
                    pass
        time.sleep(second)