import pymongo
#set up environment to connect with mongoDB database
client = pymongo.MongoClient("mongodb+srv://i")
db = client.earthquake
collection = db['all_earthquake']


import concurrent.futures
import requests
import tweepy
import pandas as pd
import reply_tweet as re
import time as t
from dotenv import load_dotenv
import os

API_KEY = os.getenv("API_KEY")
API_KEY_SECRET = os.getenv("api_key_secret")
ACCESS_TOKEN = os.getenv("access_token")
ACCESS_TOKEN_SECRET =os.getenv("access_token_secret")

auth = tweepy.OAuthHandler(API_KEY, API_KEY_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth, wait_on_rate_limit=True)
OPENDATA_KEY = os.getenv("opendata_key")

big = f'https://opendata.cwb.gov.tw/api/v1/rest/datastore/E-A0015-001?Authorization={OPENDATA_KEY}'
small = f'https://opendata.cwb.gov.tw/api/v1/rest/datastore/E-A0016-001?Authorization={OPENDATA_KEY}'


def load():
    # get data from opendata api
    response_s = requests.get(small)
    res_ch = response_s.json()
    response_b = requests.get(big)
    res_b = response_b.json()

    earthquake_time = []  # 建立一個整合大小地震的df
    description = []
    uri = []
    magnitude = []
    # 大地震
    try:
        for i in range(100):
            time = res_b['records']['earthquake'][i]['earthquakeInfo']['originTime']
            earthquake_time.append(time)

            content = res_b['records']['earthquake'][i]['reportContent']
            description.append(content)

            mag = res_b['records']['earthquake'][i]['earthquakeInfo']['magnitude']['magnitudeValue']
            magnitude.append(mag)

            u = res_b['records']['earthquake'][i]['reportImageURI']
            uri.append(u)
    except IndexError:
        pass
    # 小區域地震
    try:
        for i in range(100):
            time = res_ch['records']['earthquake'][i]['earthquakeInfo']['originTime']
            earthquake_time.append(time)

            content = res_ch['records']['earthquake'][i]['reportContent']
            description.append(content)

            mag = res_ch['records']['earthquake'][i]['earthquakeInfo']['magnitude']['magnitudeValue']
            magnitude.append(mag)

            u = res_ch['records']['earthquake'][i]['reportImageURI']
            uri.append(u)
    except IndexError:
        pass

    data = {'time': earthquake_time, 'despriction': description, 'magnitude': magnitude, 'uri': uri}
    df = pd.DataFrame(data)
    return df

def read_my_tweet():
    userID = "EarthquakeTwtw"
    tweets = api.user_timeline(screen_name=userID,
                               count=20,
                               include_rts=False,
                               tweet_mode='extended')
    posted = []
    for info in tweets:
        latest_tweet = info.full_text[:11]
        posted.append(latest_tweet)

    print("我發過的")
    print(posted, end="\n ")
    return posted

def post_new(second):
    while True:
        posted = read_my_tweet()

        df = load()

        df['time'] = pd.to_datetime(df['time'])  # 轉換為了要排序
        df_sort = df.sort_values('time', ascending=False).reset_index(drop=True)
        des = df_sort['despriction'][:6]  # 比較最近六筆資料

        all_des = []
        for i in des:
            a = i[:11]
            all_des.append(a)

        print("資料上最新")
        print(all_des, end="\n ")

        #compare the data in the API with recent tweet we just sent. If found new earthquake then tweet the new information
        a_set = set(posted)
        idx_b_minus_a = [idx for idx, val in enumerate(all_des) if val not in a_set]
        idx_b_minus_a.reverse()  #find out the index of new earthquakes and tweet according to happening  time

        print(idx_b_minus_a)

        for i in idx_b_minus_a:
            time = df_sort['time'][i]
            content = df_sort['despriction'][i]
            mag = df_sort['magnitude'][i]
            u = df_sort['uri'][i]

            api.update_status(content + u)
            print('earthquake just happened', count)


            #at the same time, load data into database
            post = {'time': time, 'despriction': content, 'magnitude': mag, 'uri': u}
            collection.insert_one(post)
    t.sleep(second)

if __name__ == '__main__':

    with concurrent.futures.ThreadPoolExecutor() as executor:
        count = 0
        f1 = executor.submit(re.reply, 10)
        #check every 10 secs if someone reply to your tweet
        f2 = executor.submit(post_new, 3600)
        #check every 60 mins if there is an earthquake just happened

        print(f1.result())
        print(f2.result())