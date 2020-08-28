import urllib
import numpy as np
import json
import datetime
import mysql.connector as connector
import pandas as pd 
import numpy as np

def iss_download(date_from, date_till, query):
    dates = []
    strdate_begin = datetime.datetime.strptime(date_from, "%Y-%m-%d")
    strdate_end = datetime.datetime.strptime(date_till, "%Y-%m-%d")
    dates = []
    x = 0
    while (strdate_begin + datetime.timedelta(x)!=strdate_end+datetime.timedelta(1)):
        dates.append((strdate_begin + datetime.timedelta(x)).strftime("%Y-%m-%d"))
        x+=1
    data = []
    for date in dates:
        response = urllib.request.urlopen(query+date+"&start=0")
        j = json.loads(response.read())
        columns = j['history']['columns']
        TOTAL = j['history.cursor']['data'][0][1]
        if TOTAL == 0: continue
        PAGESIZE = j['history.cursor']['data'][0][2]
        CURSOR = 0
        for i in j['history']['data']:
            data.append(i)
        CURSOR+=PAGESIZE
        print("TOTAL:",TOTAL)
        while True:
                #print(len(j['history']['data']))
                response = urllib.request.urlopen("http://iss.moex.com/iss/history/engines/stock/markets/shares/boards/tqbr/securities.json?date="+date+"&start="+str(CURSOR))
                j = json.loads(response.read())
                columns = j['history']['columns']
                TOTAL = j['history.cursor']['data'][0][1]
                if len(j['history']['data']) == 0: break
                PAGESIZE = j['history.cursor']['data'][0][2]
                CURSOR+=PAGESIZE
                for i in j['history']['data']:
                    data.append(i)
    return data, columns

def query_to_pd(query, columns, User = "root", Password = "1q2w3e4rq", Database = "industrymodel"):
    cnx = connector.connect(user = User, password = Password, database = Database)
    cursor = cnx.cursor()
    cursor.execute(query)
    result_raw = cursor.fetchall()
    result_array = []
    for i in result_raw:
        result_array.append(np.array(i))
    result_array = np.array(result_array)
    result_df = pd.DataFrame(result_array)
    result_df.columns = columns
    cnx.close()
    return result_df

def load_to_db(table, values, cursor):
    query = "insert into "+table+" values("
    for i in values:
        query+="\'"+str(i)+"\'"+","
    try:
        cursor.execute(query[:-1]+")")
    except Exception as e:
        print(e)
    return query[:-1]+")"