import mysql.connector
import public_bot as pu
import os


conn = mysql.connector.connect(
    host = "localhost",
    user = 'root',
    password = os.getenv("password"),
    database = 'earthquake')

cursor = conn.cursor()

#創建表格
cursor.execute("""
CREATE TABLE IF NOT EXISTS twitter (
id INT AUTO_INCREMENT,
earthquake_time TIMESTAMP,
description VARCHAR(200),
magnitude INT,
uri VARCHAR(100),
PRIMARY KEY (`id`)
);
""")
#一開始吃進API上所有資料
# for i in range(len(df)):
#     time =  df['time'][i]
#     description = df['description'][i]
#     magnitude = df['magnitude'][i]
#     uri = df['uri'][i]
#     cursor.execute(f"""
#     INSERT INTO twitter (earthquake_time, description, magnitude, uri) VALUES
#     ('{time}', '{description}', {magnitude}, '{uri}')
#     """)
#先讀取資料庫
cursor.execute("select `description` from twitter")
results = cursor.fetchall()

stored = []
for row in results[-14:]:
    stored_data = row[0][:11]
    stored.append(stored_data)

print("已儲存資料stored")
print(stored, end="\n ")

#讀api上最新資料
df =pu.load()
all_des = []
des = df['description'][-8:]  # 比較最近8筆資料
for i in des:
    a = i[:11]
    all_des.append(a)


a_set = set(stored)
all_des.reverse()
print("資料上最新all_de")
print(all_des, end="\n ")

#比對後再填資料
idx_b_minus_a = [idx for idx, val in enumerate(all_des) if val not in a_set]
idx_b_minus_a.reverse()  # 先找出發生的地震的索引值，才能依序存入
print(idx_b_minus_a)

df_sort = df.sort_values('time', ascending=False).reset_index(drop=True)

for i in idx_b_minus_a:
    time = df_sort['time'][i]
    content = df_sort['description'][i]
    mag = df_sort['magnitude'][i]
    u = df_sort['uri'][i]
    #print(time)
    cursor.execute(f"""
    INSERT INTO twitter (earthquake_time, description, magnitude, uri) VALUES
    ('{time}', '{content}', {mag}, '{u}')
    """)


conn.commit()
conn.close()