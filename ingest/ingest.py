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
from botocore.exceptions import ClientError
import logging

ticker = 'AAPL'
conn = sqlite3.connect('prices.db')
c = conn.cursor()
data = yf.Ticker(ticker).history(period="max")
data.to_sql(ticker, conn, schema=None, if_exists='replace', index=True, index_label=None, chunksize=None, dtype=None, method=None)


S3_BUCKET = os.environ.get('S3_BUCKET')

# Upload the file
s3_client = boto3.client('s3',
    aws_access_key_id=os.environ.get('AWS_ID'),
    aws_secret_access_key=os.environ.get('AWS_KEY'))

try:
    response = s3_client.upload_file("prices.db", S3_BUCKET, "prices.db")
except ClientError as e:
    logging.error(e)
