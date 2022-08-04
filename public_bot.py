import concurrent.futures
import requests
import tweepy
import pandas as pd
import reply_tweet as re
import time as t

api_key = 'AR92E'
api_key_secret = 'snwd'
access_token = 'ZfyK'
access_token_secret = '2kTCF'

auth = tweepy.OAuthHandler(api_key, api_key_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)

big = 'https://opendata.cwb.gov.tw/api/v1/rest/datastore'
small = 'https://opendata.cwb.gov.tw/api/v1/rest/datastore'

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

        post = {'time': time, 'despriction': content, 'magnitude': mag, 'uri': u}

    t.sleep(second)

if __name__ == '__main__':
    count = 0
    with concurrent.futures.ProcessPoolExecutor() as executor:
        f1 = executor.submit(re.reply, 10)
        #check every 10 secs if someone reply to your tweet
        f2 = executor.submit(post_new, 3600)
        #check every 60 mins if there is an earthquake just happened

        print(f1.result())
        print(f2.result())