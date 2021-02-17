import os
import sqlite3
import yfinance as yf
import boto3
from botocore.exceptions import ClientError
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import warnings

def download(v):
    v = v[1]
    if "^" in v:
        return
    v = v.replace("/","-")
    conn = sqlite3.connect('prices.db')
    c = conn.cursor()
    data = yf.Ticker(v).history(period="max")
    data.to_sql(v, conn, schema=None, if_exists='replace', index=True, index_label=None, chunksize=None, dtype=None, method=None)


def upload():
    S3_BUCKET = os.environ.get('S3_BUCKET')

    # Upload the file
    s3_client = boto3.client('s3',
        aws_access_key_id=os.environ.get('AWS_ID'),
        aws_secret_access_key=os.environ.get('AWS_KEY'))

    try:
        response = s3_client.upload_file("prices.db", S3_BUCKET, "prices.db")
    except ClientError as e:
        logging.error(e)

if __name__ == "__main__":
    warnings.filterwarnings("ignore")
    n_threads = 20
    df = pd.read_csv('./tickers.csv')
    ticker = df['Symbol'].iteritems()
    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        pool.map(download, ticker)
    upload()