import pandas as pd
# table=pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
# df = table[0]
# df.to_csv("S&P500.csv", columns=['Symbol'])

table=pd.read_html('https://en.wikipedia.org/wiki/NASDAQ-100#Components')
df_nas= table[3]
df_nas.to_csv("NASDAQ.csv", columns=['Ticker'])


