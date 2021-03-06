import os
import time
import datetime

import pandas as pd
import requests
import utils
import glob

UPDATE_DATE = '{}{}'.format(datetime.datetime.now().year, str(datetime.datetime.now().month).zfill(2))
SAVE_DIR = os.path.join('..', utils.DATA_DIR,
                        'trading_history_update' + '_{}{}{}'.format(datetime.datetime.now().year,
                                                                    str(datetime.datetime.now().month).zfill(2),
                                                                    str(datetime.datetime.now().day).zfill(2)
                                                                    ))
if not os.path.exists(SAVE_DIR):
    os.makedirs(SAVE_DIR)

LAST_UPDATE_DIR = glob.glob(os.path.join('..', utils.DATA_DIR, utils.TRADING_HISTORY_DIR + '*'))[-2]


def get_latest_trading_data(symbol, update_date):
    url = 'https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=csv&' \
          'date={0}&stockNo={1}'.format(update_date + '01', symbol)
    print('Request {} data from {}...'.format(update_date, url))
    request = requests.get(url)
    data = request.content.decode(utils.DECODE)
    data = data.split("\r\n")[1:-6]
    data = [x.replace('",', '').split('"')[1:] for x in data]
    print(len(data))
    if len(data) == 0:
        return []
    df = pd.DataFrame(data[1:], columns=data[0])
    df.columns = ['date', 'vol', 'val', 'open', 'high', 'low', 'close', 'change', 'transaction']
    df.date = df.date.apply(str.strip)
    for col in df.columns[3:7]:
        df[col] = df[col].str.replace(',', '')
    return df


if __name__ == '__main__':
    print("{}\n{} INFO: Start to update history trading data\n{}".format('=' * 66,
                                                                         time.strftime('%Y/%m/%d %H:%M:%S'),
                                                                         '=' * 66))
    """
    if failed, this script would run again.
    In order to avoid repeat requesting same data, skip symbols already exist in SAVE_DIR
    """
    crawled = dict(zip([int(x.split('_')[0]) for x in os.listdir(SAVE_DIR)], os.listdir(SAVE_DIR)))
    """
    For each file in latest updated data,
    read the CSV data and merge new data retrieved into it
    """
    for file_name in os.listdir(os.path.join(LAST_UPDATE_DIR)):
        # src_path = os.path.join(LAST_UPDATE_DIR, file_name)
        # df_old = pd.read_csv(src_path)
        # df_old = df_old.iloc[:, 1:]
        # df_old.to_csv(src_path, index=False)
        """
        new file name should update its end date
        """
        new_file_name = file_name.split('_')
        new_file_name[-1] = '{}.csv'.format(UPDATE_DATE)
        new_file_name = '_'.join(new_file_name)
        company_symbol = file_name.split('_')[0]
        if int(company_symbol) in crawled or int(company_symbol) in utils.SKIP_SYMBOL:
            print('{} {} crawled already'.format(time.strftime('%Y/%m/%d %H:%M:%S'), company_symbol))
            continue
        time.sleep(3)
        print('{} update {}...'.format(time.strftime('%Y/%m/%d %H:%M:%S'), company_symbol))

        src_path = os.path.join(LAST_UPDATE_DIR, file_name)
        dst_path = os.path.join(SAVE_DIR, new_file_name)
        df_old = pd.read_csv(src_path)
        df_old = df_old.astype('object')
        df_new = []
        while len(df_new) == 0:
            df_new = get_latest_trading_data(company_symbol, UPDATE_DATE)
            time.sleep(3)
        df_new = df_old.merge(df_new, how='outer')
        df_new.to_csv(dst_path, index=False)
        print('Done {}\n old {}\nupdate to {}'.format(company_symbol, src_path, dst_path))
