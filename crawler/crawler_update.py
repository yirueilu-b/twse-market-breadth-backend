import os
import time
import datetime

import pandas as pd
import requests

DATA_DIR = 'data'
SAVE_DIR = 'trading_history_{}'.format(time.strftime('%Y%m%d%H%M%S'))
os.mkdir(SAVE_DIR)
LOG_PATH = os.path.join('.', 'log_{}.txt'.format(time.strftime('%Y%m%d%H%M%S')))
DECODE = 'big5'
TARGET_DATE = '{}{}'.format(datetime.datetime.now().year, str(datetime.datetime.now().month).zfill(2))
TIME_SLEEP = 3


if __name__ == '__main__':
    print("INFO: Start to update history trading data".format(datetime.datetime.strftime(datetime.datetime.now(), '%Y/%m/%d %H:%M:%S')))
    company_list = pd.read_csv(os.path.join('..', DATA_DIR, 'company_list.csv'))
    for index, company in company_list.iterrows():
        symbol = company.symbol
        start_date = get_start_date(company.symbol, company.date, START_DATE, END_DATE)
        des_path = os.path.join('..', DATA_DIR, SAVE_DIR, '{0}_{1}_{2}.csv'.format(symbol, start_date, END_DATE))
        if os.path.exists(des_path):
            with open(LOG_PATH, 'a') as f:
                info = '{} has been crawled already, saved at {}'.format(symbol, des_path)
                f.write('SUCCEEDED: {}\nINFO: {}\n\n'.format(symbol, info))
            continue
        get_trading_history(symbol, start_date, END_DATE)

