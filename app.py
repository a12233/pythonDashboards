import streamlit as st
from polygon import RESTClient
import os
import json

def testPolygon(ticker): 
    key = os.environ['POLYGON_KEY']

    with RESTClient(key) as client:

        response = client.reference_stock_financials(ticker, limit=1, type="Y")
        attribute = ['ticker','revenuesUSD', 'marketCapitalization', 'grossProfit', 'netCashFlowFromOperations']
        res = dict.fromkeys(attribute)
        for i in attribute: 
            res[i] = response.results[0][i]
        return json.dumps(res) 

if __name__ == "__main__":
    st.json(testPolygon('AAPL'))
    st.json(testPolygon('AMZN'))
