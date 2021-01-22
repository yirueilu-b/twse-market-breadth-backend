import os
import time

import pandas as pd
import requests

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


def get_start_date(company_symbol, company_date, start, end):
    """
    some company listed on TWSE was listed later than `201001`
    must check if the start date is valid for the company
    :param company_symbol: int, the symbol of a company in TWSE
    :param company_date: str, the listed date of company
    :param start: str, default start date `201001`, could modify `START_DATE` to change this value
    :param end: str, default is current date, could modify `END_DATE` to change this value
    :return: str, the valid start date which could be used for crawling
    """
    print("Check valid start date for crawling {}...".format(company_symbol))
    listed_date = ''.join(company_date.split('/')[:2])
    if int(start) > int(listed_date):
        valid_start_date = start
        print("=" * 50 + "\n", "{} is later than {}, use {} as start date\n".format(start, listed_date, start),
              "=" * 50 + "\n")
        return valid_start_date

    start_year = int(listed_date[:4])
    start_month = int(listed_date[4:])
    end_year = int(end[:4])
    end_month = int(end[4:])
    for year in range(start_year, end_year + 1):
        sm, em = 1, 12
        if year == start_year:
            sm = max(start_month, 1)
        if year == end_year:
            em = min(end_month, 12)
        for month in range(sm, em + 1):
            time.sleep(TIME_SLEEP)
            date = str(year) + str(month).zfill(2) + '01'
            url = 'https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=csv&' \
                  'date={0}&stockNo={1}'.format(date, company_symbol)
            try:
                request = requests.get(url)
                if request.status_code == 200:
                    valid_start_date = '{}{:02d}'.format(year, month)
                    data = request.content.decode(DECODE)
                    if not data.strip():
                        raise Exception
                    print("=" * 50 + "\n", "{} is valid, use {} as start date\n".format(listed_date, listed_date),
                          "=" * 50 + "\n")
                    return valid_start_date
            except Exception as e:
                print("{} is invalid, keep trying next date".format(listed_date))
                print('ERROR:{} {}{:02d}'.format(company_symbol, year, month))
                print(e)
                continue

    return start_date


if __name__ == '__main__':
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
