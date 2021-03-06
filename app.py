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
import boto3
from botocore.exceptions import ClientError
import logging
import collections

millnames = ['',' Thousand',' M',' B',' T']

def millify(n):
    n = float(n)
    millidx = max(0,min(len(millnames)-1,
                        int(math.floor(0 if n == 0 else math.log10(abs(n))/3))))

    return '{:.0f}{}'.format(n / 10**(3 * millidx), millnames[millidx])

def testPolygon(ticker): 
    key = os.environ['POLYGON_KEY']
    t = ticker
    if ticker in ['BRK-B', 'BF-B']:
        t = ticker.replace('-','.')
    with RESTClient(key) as client:

        response = client.reference_stock_financials(t, limit=1, type="Y")
        attribute = ['ticker','revenuesUSD', 'marketCapitalization', 'grossProfit', 'netCashFlowFromOperations', 'EBITDAMargin', 'debtToEquityRatio']
        res = dict.fromkeys(attribute)
        for i in attribute: 
            if i not in ['ticker', 'EBITDAMargin', 'debtToEquityRatio']:
                res[i] = millify(response.results[0][i])
            else:
                res[i] = response.results[0][i]
        return res

def getPriceChange(ticker):
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

    timeList = [dateMinus6mo, year_start_date, dateMinus3, dateMinus5]
    # timeMap = {six_mo_ago: dateMinus6mo, three_yrs_ago : dateMinus3, five_yrs_ago : dateMinus5, test : year_start_date}
    res = {}
    outputArray = [ticker]
    # print(df.tail(1))
    # print(df.loc[df['Date'] == year_start_date])
    # print(df.tail(1))
    for time in timeList:
        if time.dayofweek == 5:
            bd = pd.tseries.offsets.BusinessDay(offset = timedelta(days = 2)) 
            time += bd 
        if time.dayofweek == 6:
            bd = pd.tseries.offsets.BusinessDay(offset = timedelta(days = 1)) 
            time += bd 
        temp_df = df.loc[df['Date'] == time]
        # print(temp_df)
        temp_df_two = df.tail(1)
        # print(temp_df)
        # res_df = temp_df_two.div(temp_df)
        # print(df.loc[df['Date'] == timeMap[time]])
        # print(temp_df_two['Close'] )
        res[time] = temp_df_two.iloc[0]['Close'] / temp_df.iloc[0]['Close']

    # ticker + '%Change from Date'
    for i,v in res.items():
        # f"{i.to_pydatetime():%Y-%m-%d}" , '{:.2%}'.format(v)
        outputArray.append('{:.2%}'.format(v))

    # st.write(res)

    conn.close()
    return outputArray

def upload():
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
        return False
    return True

def getNearestBusinessDayInPast(date, days=0, weeks=0, months=0, years=0):
    date = datetime.now()
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
    return

if __name__ == "__main__":
    sp500List = [('', 'Symbol'), ('0', 'MMM'), ('1', 'ABT'), ('2', 'ABBV'), ('3', 'ABMD'), ('4', 'ACN'), ('5', 'ATVI'),
                 ('6', 'ADBE'), ('7', 'AMD'), ('8', 'AAP'), ('9', 'AES'), ('10', 'AFL'), ('11', 'A'), ('12', 'APD'),
                 ('13', 'AKAM'), ('14', 'ALK'), ('15', 'ALB'), ('16', 'ARE'), ('17', 'ALXN'), ('18', 'ALGN'),
                 ('19', 'ALLE'), ('20', 'LNT'), ('21', 'ALL'), ('22', 'GOOGL'), ('23', 'GOOG'), ('24', 'MO'),
                 ('25', 'AMZN'), ('26', 'AMCR'), ('27', 'AEE'), ('28', 'AAL'), ('29', 'AEP'), ('30', 'AXP'),
                 ('31', 'AIG'), ('32', 'AMT'), ('33', 'AWK'), ('34', 'AMP'), ('35', 'ABC'), ('36', 'AME'),
                 ('37', 'AMGN'), ('38', 'APH'), ('39', 'ADI'), ('40', 'ANSS'), ('41', 'ANTM'), ('42', 'AON'),
                 ('43', 'AOS'), ('44', 'APA'), ('45', 'AIV'), ('46', 'AAPL'), ('47', 'AMAT'), ('48', 'APTV'),
                 ('49', 'ADM'), ('50', 'ANET'), ('51', 'AJG'), ('52', 'AIZ'), ('53', 'T'), ('54', 'ATO'),
                 ('55', 'ADSK'), ('56', 'ADP'), ('57', 'AZO'), ('58', 'AVB'), ('59', 'AVY'), ('60', 'BKR'),
                 ('61', 'BLL'), ('62', 'BAC'), ('63', 'BK'), ('64', 'BAX'), ('65', 'BDX'), ('66', 'BRK-B'),
                 ('67', 'BBY'), ('68', 'BIO'), ('69', 'BIIB'), ('70', 'BLK'), ('71', 'BA'), ('72', 'BKNG'),
                 ('73', 'BWA'), ('74', 'BXP'), ('75', 'BSX'), ('76', 'BMY'), ('77', 'AVGO'), ('78', 'BR'),
                 ('79', 'BF-B'), ('80', 'CHRW'), ('81', 'COG'), ('82', 'CDNS'), ('83', 'CPB'), ('84', 'COF'),
                 ('85', 'CAH'), ('86', 'KMX'), ('87', 'CCL'), ('88', 'CARR'), ('89', 'CTLT'), ('90', 'CAT'),
                 ('91', 'CBOE'), ('92', 'CBRE'), ('93', 'CDW'), ('94', 'CE'), ('95', 'CNC'), ('96', 'CNP'),
                 ('97', 'CERN'), ('98', 'CF'), ('99', 'SCHW'), ('100', 'CHTR'), ('101', 'CVX'), ('102', 'CMG'),
                 ('103', 'CB'), ('104', 'CHD'), ('105', 'CI'), ('106', 'CINF'), ('107', 'CTAS'), ('108', 'CSCO'),
                 ('109', 'C'), ('110', 'CFG'), ('111', 'CTXS'), ('112', 'CLX'), ('113', 'CME'), ('114', 'CMS'),
                 ('115', 'KO'), ('116', 'CTSH'), ('117', 'CL'), ('118', 'CMCSA'), ('119', 'CMA'), ('120', 'CAG'),
                 ('121', 'CXO'), ('122', 'COP'), ('123', 'ED'), ('124', 'STZ'), ('125', 'COO'), ('126', 'CPRT'),
                 ('127', 'GLW'), ('128', 'CTVA'), ('129', 'COST'), ('130', 'CCI'), ('131', 'CSX'), ('132', 'CMI'),
                 ('133', 'CVS'), ('134', 'DHI'), ('135', 'DHR'), ('136', 'DRI'), ('137', 'DVA'), ('138', 'DE'),
                 ('139', 'DAL'), ('140', 'XRAY'), ('141', 'DVN'), ('142', 'DXCM'), ('143', 'FANG'), ('144', 'DLR'),
                 ('145', 'DFS'), ('146', 'DISCA'), ('147', 'DISCK'), ('148', 'DISH'), ('149', 'DG'), ('150', 'DLTR'),
                 ('151', 'D'), ('152', 'DPZ'), ('153', 'DOV'), ('154', 'DOW'), ('155', 'DTE'), ('156', 'DUK'),
                 ('157', 'DRE'), ('158', 'DD'), ('159', 'DXC'), ('160', 'EMN'), ('161', 'ETN'), ('162', 'EBAY'),
                 ('163', 'ECL'), ('164', 'EIX'), ('165', 'EW'), ('166', 'EA'), ('167', 'EMR'), ('168', 'ETR'),
                 ('169', 'EOG'), ('170', 'EFX'), ('171', 'EQIX'), ('172', 'EQR'), ('173', 'ESS'), ('174', 'EL'),
                 ('175', 'ETSY'), ('176', 'EVRG'), ('177', 'ES'), ('178', 'RE'), ('179', 'EXC'), ('180', 'EXPE'),
                 ('181', 'EXPD'), ('182', 'EXR'), ('183', 'XOM'), ('184', 'FFIV'), ('185', 'FB'), ('186', 'FAST'),
                 ('187', 'FRT'), ('188', 'FDX'), ('189', 'FIS'), ('190', 'FITB'), ('191', 'FE'), ('192', 'FRC'),
                 ('193', 'FISV'), ('194', 'FLT'), ('195', 'FLIR'), ('196', 'FLS'), ('197', 'FMC'), ('198', 'F'),
                 ('199', 'FTNT'), ('200', 'FTV'), ('201', 'FBHS'), ('202', 'FOXA'), ('203', 'FOX'), ('204', 'BEN'),
                 ('205', 'FCX'), ('206', 'GPS'), ('207', 'GRMN'), ('208', 'IT'), ('209', 'GD'), ('210', 'GE'),
                 ('211', 'GIS'), ('212', 'GM'), ('213', 'GPC'), ('214', 'GILD'), ('215', 'GL'), ('216', 'GPN'),
                 ('217', 'GS'), ('218', 'GWW'), ('219', 'HAL'), ('220', 'HBI'), ('221', 'HIG'), ('222', 'HAS'),
                 ('223', 'HCA'), ('224', 'PEAK'), ('225', 'HSIC'), ('226', 'HSY'), ('227', 'HES'), ('228', 'HPE'),
                 ('229', 'HLT'), ('230', 'HFC'), ('231', 'HOLX'), ('232', 'HD'), ('233', 'HON'), ('234', 'HRL'),
                 ('235', 'HST'), ('236', 'HWM'), ('237', 'HPQ'), ('238', 'HUM'), ('239', 'HBAN'), ('240', 'HII'),
                 ('241', 'IEX'), ('242', 'IDXX'), ('243', 'INFO'), ('244', 'ITW'), ('245', 'ILMN'), ('246', 'INCY'),
                 ('247', 'IR'), ('248', 'INTC'), ('249', 'ICE'), ('250', 'IBM'), ('251', 'IP'), ('252', 'IPG'),
                 ('253', 'IFF'), ('254', 'INTU'), ('255', 'ISRG'), ('256', 'IVZ'), ('257', 'IPGP'), ('258', 'IQV'),
                 ('259', 'IRM'), ('260', 'JKHY'), ('261', 'J'), ('262', 'JBHT'), ('263', 'SJM'), ('264', 'JNJ'),
                 ('265', 'JCI'), ('266', 'JPM'), ('267', 'JNPR'), ('268', 'KSU'), ('269', 'K'), ('270', 'KEY'),
                 ('271', 'KEYS'), ('272', 'KMB'), ('273', 'KIM'), ('274', 'KMI'), ('275', 'KLAC'), ('276', 'KHC'),
                 ('277', 'KR'), ('278', 'LB'), ('279', 'LHX'), ('280', 'LH'), ('281', 'LRCX'), ('282', 'LW'),
                 ('283', 'LVS'), ('284', 'LEG'), ('285', 'LDOS'), ('286', 'LEN'), ('287', 'LLY'), ('288', 'LNC'),
                 ('289', 'LIN'), ('290', 'LYV'), ('291', 'LKQ'), ('292', 'LMT'), ('293', 'L'), ('294', 'LOW'),
                 ('295', 'LUMN'), ('296', 'LYB'), ('297', 'MTB'), ('298', 'MRO'), ('299', 'MPC'), ('300', 'MKTX'),
                 ('301', 'MAR'), ('302', 'MMC'), ('303', 'MLM'), ('304', 'MAS'), ('305', 'MA'), ('306', 'MKC'),
                 ('307', 'MXIM'), ('308', 'MCD'), ('309', 'MCK'), ('310', 'MDT'), ('311', 'MRK'), ('312', 'MET'),
                 ('313', 'MTD'), ('314', 'MGM'), ('315', 'MCHP'), ('316', 'MU'), ('317', 'MSFT'), ('318', 'MAA'),
                 ('319', 'MHK'), ('320', 'TAP'), ('321', 'MDLZ'), ('322', 'MNST'), ('323', 'MCO'), ('324', 'MS'),
                 ('325', 'MOS'), ('326', 'MSI'), ('327', 'MSCI'), ('328', 'NDAQ'), ('329', 'NOV'), ('330', 'NTAP'),
                 ('331', 'NFLX'), ('332', 'NWL'), ('333', 'NEM'), ('334', 'NWSA'), ('335', 'NWS'), ('336', 'NEE'),
                 ('337', 'NLSN'), ('338', 'NKE'), ('339', 'NI'), ('340', 'NSC'), ('341', 'NTRS'), ('342', 'NOC'),
                 ('343', 'NLOK'), ('344', 'NCLH'), ('345', 'NRG'), ('346', 'NUE'), ('347', 'NVDA'), ('348', 'NVR'),
                 ('349', 'ORLY'), ('350', 'OXY'), ('351', 'ODFL'), ('352', 'OMC'), ('353', 'OKE'), ('354', 'ORCL'),
                 ('355', 'OTIS'), ('356', 'PCAR'), ('357', 'PKG'), ('358', 'PH'), ('359', 'PAYX'), ('360', 'PAYC'),
                 ('361', 'PYPL'), ('362', 'PNR'), ('363', 'PBCT'), ('364', 'PEP'), ('365', 'PKI'), ('366', 'PRGO'),
                 ('367', 'PFE'), ('368', 'PM'), ('369', 'PSX'), ('370', 'PNW'), ('371', 'PXD'), ('372', 'PNC'),
                 ('373', 'POOL'), ('374', 'PPG'), ('375', 'PPL'), ('376', 'PFG'), ('377', 'PG'), ('378', 'PGR'),
                 ('379', 'PLD'), ('380', 'PRU'), ('381', 'PEG'), ('382', 'PSA'), ('383', 'PHM'), ('384', 'PVH'),
                 ('385', 'QRVO'), ('386', 'PWR'), ('387', 'QCOM'), ('388', 'DGX'), ('389', 'RL'), ('390', 'RJF'),
                 ('391', 'RTX'), ('392', 'O'), ('393', 'REG'), ('394', 'REGN'), ('395', 'RF'), ('396', 'RSG'),
                 ('397', 'RMD'), ('398', 'RHI'), ('399', 'ROK'), ('400', 'ROL'), ('401', 'ROP'), ('402', 'ROST'),
                 ('403', 'RCL'), ('404', 'SPGI'), ('405', 'CRM'), ('406', 'SBAC'), ('407', 'SLB'), ('408', 'STX'),
                 ('409', 'SEE'), ('410', 'SRE'), ('411', 'NOW'), ('412', 'SHW'), ('413', 'SPG'), ('414', 'SWKS'),
                 ('415', 'SLG'), ('416', 'SNA'), ('417', 'SO'), ('418', 'LUV'), ('419', 'SWK'), ('420', 'SBUX'),
                 ('421', 'STT'), ('422', 'STE'), ('423', 'SYK'), ('424', 'SIVB'), ('425', 'SYF'), ('426', 'SNPS'),
                 ('427', 'SYY'), ('428', 'TMUS'), ('429', 'TROW'), ('430', 'TTWO'), ('431', 'TPR'), ('432', 'TGT'),
                 ('433', 'TEL'), ('434', 'FTI'), ('435', 'TDY'), ('436', 'TFX'), ('437', 'TER'), ('438', 'TXN'),
                 ('439', 'TXT'), ('440', 'TMO'), ('441', 'TIF'), ('442', 'TJX'), ('443', 'TSCO'), ('444', 'TT'),
                 ('445', 'TDG'), ('446', 'TRV'), ('447', 'TFC'), ('448', 'TWTR'), ('449', 'TYL'), ('450', 'TSN'),
                 ('451', 'UDR'), ('452', 'ULTA'), ('453', 'USB'), ('454', 'UAA'), ('455', 'UA'), ('456', 'UNP'),
                 ('457', 'UAL'), ('458', 'UNH'), ('459', 'UPS'), ('460', 'URI'), ('461', 'UHS'), ('462', 'UNM'),
                 ('463', 'VLO'), ('464', 'VAR'), ('465', 'VTR'), ('466', 'VRSN'), ('467', 'VRSK'), ('468', 'VZ'),
                 ('469', 'VRTX'), ('470', 'VFC'), ('471', 'VIAC'), ('472', 'VTRS'), ('473', 'V'), ('474', 'VNT'),
                 ('475', 'VNO'), ('476', 'VMC'), ('477', 'WRB'), ('478', 'WAB'), ('479', 'WMT'), ('480', 'WBA'),
                 ('481', 'DIS'), ('482', 'WM'), ('483', 'WAT'), ('484', 'WEC'), ('485', 'WFC'), ('486', 'WELL'),
                 ('487', 'WST'), ('488', 'WDC'), ('489', 'WU'), ('490', 'WRK'), ('491', 'WY'), ('492', 'WHR'),
                 ('493', 'WMB'), ('494', 'WLTW'), ('495', 'WYNN'), ('496', 'XEL'), ('497', 'XRX'), ('498', 'XLNX'),
                 ('499', 'XYL'), ('500', 'YUM'), ('501', 'ZBRA'), ('502', 'ZBH'), ('503', 'ZION'), ('504', 'ZTS')]
    outputColumnsGrowth = ['Ticker', '6 month', 'YTD', '3Y', '5Y']
    outputColumnsFundamentals = ['Ticker', 'revenuesUSD', 'marketCapitalization', 'grossProfit', 'netCashFlowFromOperations']
    fundamentalData = []
    growthData = []
    for ticker in sp500List[1:]:
        ticker = ticker[1]
        if ticker in ['GOOG', 'CARR','DISCK','FRC', 'LUMN', 'NWS', 'OTIS', 'UA', 'VTRS','VNT']:
            continue
        fundamentalData.append(testPolygon(ticker))
        # growthData.append(getPriceChange(ticker))

    # dfGrowth = pd.DataFrame(growthData, columns = outputColumnsGrowth)
    dfFundamental = pd.DataFrame(fundamentalData)
    # st.dataframe(dfGrowth, width = 2000, height = 500)
    # st.dataframe(dfFundamental, width = 50000, height = 5000)

    # col1, col2 = st.beta_columns(2)
    # with col1: gi
    #     st.dataframe(dfGrowth, width = 2000, height = 500)
    # with col2: 
    #     st.dataframe(dfFundamental, width = 50000, height = 5000)
    res = collections.defaultdict(list)

    conn = sqlite3.connect('prices.db')
    c = conn.cursor()
    # data = yf.Ticker('BF-B').history(period="max")
    # data.to_sql('BF-B', conn, schema=None, if_exists='replace', index=True, index_label=None, chunksize=None, dtype=None,
    #             method=None)

    for symbol in sp500List[1:]:
        symbol = symbol[1]
        # c.execute(f"""SELECT count(*) FROM sqlite_master WHERE type=\'table\' AND name=\'{symbol}\';""")
        # flag = c.fetchall()
        data = pd.read_sql(f"""SELECT * FROM '{symbol}'""", conn)
        # data = pdr.get_data_yahoo(symbol, 2020, '20210215')
        lastPrice = data['Close'].tail(1).item()
        sma20 = data.tail(20)['Close'].mean()
        sma60 = data.tail(60)['Close'].mean()
        sma120 = data.tail(120)['Close'].mean()

        res[symbol] = {"Symbol":symbol, "C/S": lastPrice / sma20, "S/M": sma20 / sma60, "M/L": sma60 / sma120}

    # tech = pd.DataFrame.from_dict(res).T
    # out = dfFundamental.set_index('Ticker').join(pd.DataFrame.from_dict(res).T.set_index('Symbol'))
    st.dataframe(pd.DataFrame.from_dict(res).T.set_index('Symbol'))
    # st.dataframe(out)
    # upload()
