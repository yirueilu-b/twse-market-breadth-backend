import os
import time

import pandas as pd
import requests

from crawler import utils


def get_index_history(start, end):
    """
    :param start: str, date in `yyyymm`, for example: 202001 represent 2020 JAN
    :param end: str, same format as `start`
    :return: None, data saved as CSV files in `data` directory
    """
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
            time.sleep(utils.TIME_SLEEP)
            date = str(year) + str(month).zfill(2) + '01'
            url = 'https://www.twse.com.tw/exchangeReport/FMTQIK?response=csv&date={0}'.format(date)
            print('Request {} data from {}...'.format(date, url))

            try:
                request = requests.get(url)
                data = request.content.decode(utils.DECODE)
                data = data.split("\r\n")[1:-5]
                data = [x.replace('",', '').split('"')[1:] for x in data]

                if date[:-2] == start:
                    df_all = pd.DataFrame(data[1:], columns=data[0])
                else:
                    temp = pd.DataFrame(data[1:], columns=data[0])
                    df_all = pd.concat([df_all, temp])

                print('{} done'.format(date))
            except Exception as e:

                print('FAILED: {}'.format(date))
                print('ERROR: {}'.format(e))
                with open(utils.LOG_PATH, 'a') as file:
                    file.write('ERROR: {} {} {}\n\n'.format(date, url, e))
                continue
    df_all.columns = ['date', 'vol', 'val', 'transaction', 'close', 'change']
    df_all.date = df_all.date.apply(str.strip)
    df_all.close = df_all.close.str.replace(',', '').astype('float')
    df_all.change = df_all.change.astype('float')

    des = os.path.join('..', utils.DATA_DIR, utils.INDEX_HISTORY_DIR,
                       '{}_{}.csv'.format(start, end))
    df_all.to_csv(des, index=False)


if __name__ == '__main__':
    time_now = time.strftime('%Y/%m/%d %H:%M:%S')
    start_date = '201001'

    des_path = os.path.join('..', utils.DATA_DIR, utils.INDEX_HISTORY_DIR,
                            'index_{}_{}.csv'.format(start_date, utils.END_DATE))
    print('{}\n Start crawling {} from {} to {}\n{}'.format('=' * 50, time_now, start_date,
                                                            utils.END_DATE, '=' * 50))
    get_index_history(start_date, utils.END_DATE)
