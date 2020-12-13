import os
import json
import math
import sqlite3
import yfinance as yf
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import numpy as np
import boto3
import pandas as pd
from pandas import DataFrame, Series
from pandas.tseries.offsets import BDay
from pandas.tseries import offsets


ticker = 'AAPL'
conn = sqlite3.connect('prices.db')
c = conn.cursor()
data = yf.Ticker(ticker).history(period="max")
data.to_sql(ticker, conn, schema=None, if_exists='replace',lindex=True, index_label=None, chunksize=None, dtype=None, method=None)


S3_BUCKET = os.environ.get('S3_BUCKET')


s3 = boto3.client('s3')

presigned_post = s3.generate_presigned_post(
Bucket = S3_BUCKET,
Key = 'test',
Fields = {"acl": "public-read", "Content-Type": '.db'},
Conditions = [
    {"acl": "public-read"},
    {"Content-Type": '.db'} 
],
    ExpiresIn = 3600
)

