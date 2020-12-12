import streamlit as st
from polygon import RESTClient
import os
import json
import math

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

if __name__ == "__main__":
    st.json(testPolygon('AAPL'))
    st.json(testPolygon('AMZN'))
