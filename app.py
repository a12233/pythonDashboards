import streamlit as st
from polygon import RESTClient
import os
import json
import math
import sqlite3
import yfinance as yf
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import numpy as np

import pandas as pd
from pandas import DataFrame, Series
from pandas.tseries.offsets import BDay
from pandas.tseries import offsets

millnames = ['',' Thousand',' Million',' Billion',' Trillion']

def millify(n):
    n = float(n)
    millidx = max(0,min(len(millnames)-1,
                        int(math.floor(0 if n == 0 else math.log10(abs(n))/3))))

    return '{:.0f}{}'.format(n / 10**(3 * millidx), millnames[millidx])

def testPolygon(ticker): 
    key = os.environ['POLYGON_KEY']

    with RESTClient(key) as client:

        response = client.reference_stock_financials(ticker, limit=1, type="Y")
        attribute = ['ticker','revenuesUSD', 'marketCapitalization', 'grossProfit', 'netCashFlowFromOperations']
        res = dict.fromkeys(attribute)
        for i in attribute: 
            if i != 'ticker':
                res[i] = millify(response.results[0][i])
            else: 
                res[i] = response.results[0][i]
        return json.dumps(res) 

def getPriceChange(ticker):
    cache = {}
    conn = sqlite3.connect('prices.db')
    c = conn.cursor()
    c.execute(f"""SELECT count(*) FROM sqlite_master WHERE type=\'table\' AND name=\'{ticker}\';""")
    flag = c.fetchall() 
    if flag[0][0] == 0:
        data = yf.Ticker(ticker).history(period="max")
        data.to_sql(ticker, conn, schema=None, if_exists='replace', index=True, index_label=None, chunksize=None, dtype=None, method=None)

    df = pd.read_sql(f"""SELECT * FROM {ticker}""", conn)
    df['Date'] = pd.to_datetime(df['Date'])
    # df['date'] = df['Date'].to_pydatetime()

    six_mo_ago = datetime.now() - relativedelta(month=6)
    three_yrs_ago = datetime.now() - relativedelta(years=3)
    five_yrs_ago = datetime.now() - relativedelta(years=5)
    year_start = "2020-01-02"
    test = datetime.strptime(year_start, '%Y-%m-%d')

    three_years = three_yrs_ago.strftime("%Y-%m-%d")
    five_years = five_yrs_ago.strftime("%Y-%m-%d")
    six_mo = six_mo_ago.strftime("%Y-%m-%d")

    dateMinus3 = pd.to_datetime(three_years, format="%Y-%m-%d")
    dateMinus5 = pd.to_datetime(five_years, format="%Y-%m-%d")
    dateMinus6mo = pd.to_datetime(six_mo, format="%Y-%m-%d")
    year_start_date = pd.to_datetime(test, format="%Y-%m-%d")

    timeList = [dateMinus6mo, dateMinus3, dateMinus5, year_start_date]
    # timeMap = {six_mo_ago: dateMinus6mo, three_yrs_ago : dateMinus3, five_yrs_ago : dateMinus5, test : year_start_date}
    res = {dateMinus6mo: 0, dateMinus3 : 0, dateMinus5 : 0, year_start_date : 0}
    # print(df.tail(1))
    # print(df.loc[df['Date'] == year_start_date])
    # print(df.tail(1))
    for time in timeList:
        if time.dayofweek == 5:
            bd = pd.tseries.offsets.BusinessDay(offset = timedelta(days = 2)) 
            time += bd 
        if time.day == 6:
            bd = pd.tseries.offsets.BusinessDay(offset = timedelta(days = 1)) 
            time += bd 
        temp_df = df.loc[df['Date'] == time]
        temp_df_two = df.tail(1)
        # print(temp_df)
        # res_df = temp_df_two.div(temp_df)
        # print(df.loc[df['Date'] == timeMap[time]])
        # print(temp_df_two['Close'] )
        res[time] = temp_df_two.iloc[0]['Close'] / temp_df.iloc[0]['Close']

    ticker + '%Change from Date'
    for i,v in res.items(): 
        f"{i.to_pydatetime():%Y-%m-%d}" , '{:.2%}'.format(v)

    # st.write(res)

    conn.close()
    return 


if __name__ == "__main__":
    sp500List = ['AAPL', 'AMZN', 'GOOGL', 'TSLA', 'MSFT']
    for ticker in sp500List: 
        st.json(testPolygon(ticker))
        getPriceChange(ticker)