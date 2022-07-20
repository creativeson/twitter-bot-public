import pymongo

client = pymongo.MongoClient("mongodb+srv://<secret>@cluster0.7wpz3kg.mongodb.net/?retryWrites=true&w=majority")
db = client.earthquake
collection = db['all_earthquake']

import requests
import tweepy
import time
import pandas as pd


api_key = secret
api_key_secret = secret
access_token = secret
access_token_secret = secret

auth = tweepy.OAuthHandler(api_key, api_key_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)

big = secret
big_en = secret
small = secret
small_en = secret


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


def post_new():
    userID = "EarthquakeTwtw"
    tweets = api.user_timeline(screen_name=userID,
                               # 200 is the maximum allowed count
                               count=10,
                               include_rts=False,
                               # Necessary to keep full_text
                               # otherwise only the first 140 words are extracted
                               tweet_mode='extended')
    posted = []
    for info in tweets:
        latest_tweet = info.full_text[:11]
        posted.append(latest_tweet)

    print("我發過的")
    print(posted, end="\n ")

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

    a_set = set(posted)
    idx_b_minus_a = [idx for idx, val in enumerate(all_des) if val not in a_set]
    idx_b_minus_a.reverse()  # 先找出發生的地震的索引值，才能依序發文

    print(idx_b_minus_a)

    for i in idx_b_minus_a:
        time = df_sort['time'][i]
        content = df_sort['despriction'][i]
        mag = df_sort['magnitude'][i]
        u = df_sort['uri'][i]

        api.update_status(content + u, count)
        print('earthquake just happened', count)

        post = {'time': time, 'despriction': content, 'magnitude': mag, 'uri': u}
        collection.insert_one(post)


count = 0
while True:
    post_new()
    print(count)
    count += 1
    time.sleep(1800)
