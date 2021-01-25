import os
import time

import requests

DATA_DIR = 'data'
TRADING_HISTORY_DIR = 'trading_history'
MARKET_BREADTH_DIR = 'market_breadth'
LOG_PATH = os.path.join('.', 'log_{}.txt'.format(time.strftime('%Y%m%d%H%M%S')))
DECODE = 'big5'
START_DATE, END_DATE = '201001', '202101'
TIME_SLEEP = 3
SKIP_SYMBOL = [6452, 6131]


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
    valid_start_date = start
    print("Check valid start date for crawling {}...".format(company_symbol))
    listed_date = ''.join(company_date.split('/')[:2])
    if int(start) > int(listed_date):
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

    return valid_start_date
