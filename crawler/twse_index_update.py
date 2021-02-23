import os
import time
import datetime
import glob

import pandas as pd
import requests

import utils

# yyyymm
UPDATE_DATE = '{}{}'.format(datetime.datetime.now().year, str(datetime.datetime.now().month).zfill(2))


def get_index_history(update_date):
    """
    :param start: str, date in `yyyymm`, for example: 202001 represent 2020 JAN
    :param end: str, same format as `start`
    :return: None, data saved as CSV files in `data` directory
    """
    url = 'https://www.twse.com.tw/exchangeReport/FMTQIK?response=csv&date={0}'.format(update_date)
    print('Request {} data from {}...'.format(update_date, url))
    request = requests.get(url)
    data = request.content.decode(utils.DECODE)
    data = data.split("\r\n")[1:-5]
    data = [x.replace('",', '').split('"')[1:] for x in data]
    print(len(data))
    if len(data) == 0:
        return []
    df = pd.DataFrame(data[1:], columns=data[0])
    df.columns = ['date', 'vol', 'val', 'transaction', 'close', 'change']
    df.date = df.date.apply(str.strip)
    df.close = df.close.str.replace(',', '').astype('float')
    df.change = df.change.str.replace(',', '').astype('float')
    return df


if __name__ == '__main__':
    src_path = glob.glob(os.path.join('..', utils.DATA_DIR, utils.INDEX_HISTORY_DIR, '*'))[-1]
    print(src_path)
    dst_path = src_path.replace(src_path.split('_')[-1].split('.')[0],
                                UPDATE_DATE + str(datetime.datetime.now().day).zfill(2))
    print(dst_path)
    df_old = pd.read_csv(src_path)
    df_old = df_old.astype('object')
    # print(df_old.tail())
    df_new = get_index_history(UPDATE_DATE)
    df_new = df_old.merge(df_new, how='outer')
    df_new['pct_change'] = df_new.close.pct_change()*100
    # print(df_new.tail())
    df_new.to_csv(dst_path, index=False)
