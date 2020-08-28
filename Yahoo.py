import numpy as np
import pandas as pd
import urllib.request
import json
from bs4 import BeautifulSoup
import mysql.connector as connector
from Procedures import query_to_pd

def pull_SecurityInfo(ticker):
    response = urllib.request.urlopen("http://iss.moex.com/iss/securities/"+ticker+".json")
    j = json.loads(response.read())
    columns = j['description']['columns']
    data = []
    for i in j['description']['data']:
            data.append(np.array(i))
            #print(data[-1])
    data = np.array(data)
    data_df = pd.DataFrame(data)
    data_df.columns = columns
    return data_df

def pushtodb_SecurityInfo(ticker,cnx):
    data = "values(\'"+ticker+"\',"
    columns = ["NAME", "SHORTNAME", "ISSUESIZE", "FACEVALUE", "FACEUNIT", "ISSUEDATE","LATNAME","LISTLEVEL","ISQUALIFIEDINVESTORS","GROUP","TYPE","GROUPNAME","EMITTER_ID"]
    SecInfo = pull_SecurityInfo(ticker).to_numpy()
    if SecInfo[0][2]!=ticker:
        print(ticker,"- no data")
        return False
    for i in SecInfo:
        if (i[0] in columns):
            data+='\''+i[2]+'\','
    #data[-1]=')'
    try:
        query = "insert into industrymodel.moex_sec_spec "+ data[:-1]+')'
        #print(query)
        cursor = cnx.cursor()
        cursor.execute(query)
        cnx.commit()
    except Exception as e:
        print("ERROR",e)
        return False
    return True

def yahoo_pull_company(ticker,date,cnx):
    response = urllib.request.urlopen("https://finance.yahoo.com/quote/"+ticker+".ME/profile")
    print("https://finance.yahoo.com/quote/"+ticker+".ME/profile")
    resp = response.read() 
    doc = BeautifulSoup(resp,"html.parser")
    tag = doc.b
    name = ''
    industry = ''
    employees = ''
    query = "insert into industrymodel.yahoo_issuers_profile values(\'"+ticker+"\',\'"+date+"\',"
    data = ""
    try:
        name = doc.find_all('h3')[0].text
        sector = doc.find_all("span", {"class":"Fw(600)"})[0].text
    except:
        print(ticker, "failed")
        return
    else:
        if sector == '':
            sector = 'NULL'
    data+='\''+name+'\',\''+sector+'\''
    try: 
        industry = doc.find_all("span", {"class":"Fw(600)"})[1].text
    except:
        data+=', NULL'
    else:
        if industry == '':
            data+=', NULL'
        else:
            data+=',\''+industry+'\''
    try:
        employees = doc.find_all("span", {"class":"Fw(600)"})[2].text
    except:
        data+=', NULL'
    else:
        if employees == '':
            data+=', NULL'
        else:
            employees = employees.replace(",","")
            data+=',\''+employees+'\''
    try:
        query += data + ")"
        cursor = cnx.cursor()
        cursor.execute(query)
        cnx.commit()
    except Exception as e:
        print(e)

#query = "SELECT Distinct ticker FROM industrymodel.moex_shares"
#columns = ["ticker"]
#tickers = query_to_pd(query, columns)
#tickers = tickers.to_numpy()

#pushing security info to db
#for i in tickers:
#   cnx = connector.connect(user = "root", password = "1q2w3e4rq", database = "industrymodel")
#    pushtodb_SecurityInfo(i[0], cnx)

##yahoo_profile pull and push into db
#for i in tickers:
#    cnx = connector.connect(user = "root", password = "1q2w3e4rq", database = "industrymodel")
#    yahoo_pull_company(i[0],"2020-06-01",cnx)