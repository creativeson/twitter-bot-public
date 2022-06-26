import requests
import tweepy
import time
import pandas as pd


api_key = my_secret
api_key_secret = my_secret
access_token = my_secret
access_token_secret = my_secret

auth = tweepy.OAuthHandler(api_key, api_key_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit = True)

big = opendata_url1
big_en = opendata_url2
small = opendata_url3
small_en = opendataurl4

def load(): #get data from central weather opendata api
    response_s = requests.get(small)
    res_ch = response_s.json()
    response_b = requests.get(big)
    res_b = response_b.json()

#create a df that combine big and small earthquakes    
    earthquake_time = [] 
    description = []
    uri = []
    magtitude = []
    #big earthquake
    try:
        for i in range(100):
            time = res_b['records']['earthquake'][i]['earthquakeInfo']['originTime']
            earthquake_time.append(time)

            content = res_b['records']['earthquake'][i]['reportContent']
            description.append(content)

            mag = res_b['records']['earthquake'][i]['earthquakeInfo']['magnitude']['magnitudeValue']
            magtitude.append(mag)

            u = res_b['records']['earthquake'][i]['reportImageURI']
            uri.append(u)
    except IndexError:
        pass
    #small earthquake
    try:
        for i in range(100):
            time = res_ch['records']['earthquake'][i]['earthquakeInfo']['originTime']
            earthquake_time.append(time)

            content = res_ch['records']['earthquake'][i]['reportContent']
            description.append(content)

            mag = res_ch['records']['earthquake'][i]['earthquakeInfo']['magnitude']['magnitudeValue']
            magtitude.append(mag)

            u = res_ch['records']['earthquake'][i]['reportImageURI']
            uri.append(u)
    except IndexError:
        pass

    data = {'time':earthquake_time, 'despriction':description, 'magtitude' : magtitude, 'uri':uri }
    df = pd.DataFrame(data)
    return df


def post_new():
    userID = "EarthquakeTwtw"
    tweets = api.user_timeline(screen_name=userID,
                           # 200 is the maximum allowed count
                           count=10,
                           include_rts = False,
                           # Necessary to keep full_text
                           # otherwise only the first 140 words are extracted
                           tweet_mode = 'extended')
    posted = []
    for info in tweets:
        latest_tweet = info.full_text[:11]
        posted.append(latest_tweet)

    print("我發過的")
    print(posted, end = "\n ")

    df = load()

    df['time'] = pd.to_datetime(df['time']) #change to datetime in order to sort
    df_sort = df.sort_values('time', ascending = False).reset_index(drop = True)
    

    des = df_sort['despriction'][:6] #compare  6 rows of data from opendata

    all_des = []
    for i in des:
        a = i[:11]
        all_des.append(a)

    print("資料上最新")
    print(all_des, end = "\n ")

    a_set = set(posted)
    idx_b_minus_a = [idx for idx, val in enumerate(all_des) if val not in a_set]
    idx_b_minus_a.reverse() 
#find out recent earthquake's index and in the order of happening time
    
    print(idx_b_minus_a) #the index of earthquake that are new and without tweet before

    for i in idx_b_minus_a:
        api.update_status(df_sort['despriction'][i] +df_sort['uri'][i], count)
        #print(df_sort['despriction'][i] +df_sort['uri'][i], count)
        print('earthquake just happened', count)

count = 0
while True:
    post_new()
    print(count)
    count += 1
    time.sleep(600)