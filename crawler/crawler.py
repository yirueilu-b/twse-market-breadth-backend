import os
import time

import pandas as pd
import requests

from crawler import utils

DATA_DIR = 'data'
SAVE_DIR = 'trading_history'
LOG_PATH = os.path.join('.', 'log_{}.txt'.format(time.strftime('%Y%m%d%H%M%S')))
DECODE = 'big5'
START_DATE, END_DATE = '201001', '202101'
TIME_SLEEP = 3


def get_trading_history(company_symbol, start, end):
    """
    :param company_symbol: int, the symbol of a company in TWSE
    :param start: str, date in `yyyymm`, for example: 202001 represent 2020 JAN
    :param end: str, same format as `start`
    :return: None, data saved as CSV files in `data` directory
    """
    print('{}\nStart crawling {} from {} to {}\n{}'.format('=' * 50, company_symbol, start, end, '=' * 50))
    start_year = int(start[:4])
    start_month = int(start[4:])
    end_year = int(end[:4])
    end_month = int(end[4:])
    df_all = None
    for year in range(start_year, end_year + 1):
        if year == start_year:
            sm = start_month
        else:
            sm = 1
        if year == end_year:
            em = end_month
        else:
            em = 12
        for month in range(sm, em + 1):
            time.sleep(TIME_SLEEP)
            date = str(year) + str(month).zfill(2) + '01'
            url = 'https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=csv&' \
                  'date={0}&stockNo={1}'.format(date, company_symbol)
            print('Request {} data from {}...'.format(date, url))

            try:
                request = requests.get(url)
                data = request.content.decode(DECODE)
                data = data.split("\r\n")[1:-6]
                data = [x.replace('",', '').split('"')[1:] for x in data]
                if date[:-2] == start:
                    df_all = pd.DataFrame(data[1:], columns=data[0])
                else:
                    temp = pd.DataFrame(data[1:], columns=data[0])
                    df_all = pd.concat([df_all, temp])
                print('{} done'.format(date))
            except Exception as e:
                print('FAILED: {} {}'.format(symbol, date))
                print('ERROR: {}'.format(e))
                with open(LOG_PATH, 'a') as file:
                    file.write('FAILED: {}\nERROR: {} {} {}\n\n'.format(symbol, date, url, e))
                return

    df_all.columns = ['date', 'vol', 'val', 'open', 'high', 'low', 'close', 'change', 'transaction']
    df_all.date = df_all.date.apply(str.strip)
    des = os.path.join('..', DATA_DIR, SAVE_DIR, '{0}_{1}_{2}.csv'.format(company_symbol, start, end))
    df_all.to_csv(des, index=False)
    with open(LOG_PATH, 'a') as file:
        log_info = '{} crawled successfully, saved in {}'.format(symbol, des_path)
        file.write('SUCCEEDED: {}\nINFO: {}\n\n'.format(symbol, log_info))


if __name__ == '__main__':
    company_list = pd.read_csv(os.path.join('..', DATA_DIR, 'company_list.csv'))
    for index, company in company_list.iterrows():
        symbol = company.symbol
        start_date = utils.get_start_date(company.symbol, company.date, START_DATE, END_DATE)
        des_path = os.path.join('..', DATA_DIR, SAVE_DIR, '{0}_{1}_{2}.csv'.format(symbol, start_date, END_DATE))
        if os.path.exists(des_path):
            with open(LOG_PATH, 'a') as f:
                info = '{} has been crawled already, saved at {}'.format(symbol, des_path)
                f.write('SUCCEEDED: {}\nINFO: {}\n\n'.format(symbol, info))
            continue
        get_trading_history(symbol, start_date, END_DATE)
