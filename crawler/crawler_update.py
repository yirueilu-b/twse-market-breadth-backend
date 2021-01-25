import os
import time
import datetime

import pandas as pd
import requests
from crawler import utils

TARGET_DATE = '{}{}'.format(datetime.datetime.now().year, str(datetime.datetime.now().month).zfill(2))
SAVE_DIR = os.path.join('..', utils.DATA_DIR, 'trading_history_{}'.format(time.strftime('%Y%m%d%H%M%S')))
os.mkdir(SAVE_DIR)


def get_latest_trading_data(symbol):
    return symbol


if __name__ == '__main__':
    print("{}\n{} INFO: Start to update history trading data\n{}".format('=' * 66,
                                                                         datetime.datetime.strftime(
                                                                             datetime.datetime.now(),
                                                                             '%Y/%m/%d %H:%M:%S'),
                                                                         '=' * 66))
    for file_name in os.listdir(os.path.join('..', utils.DATA_DIR, utils.TRADING_HISTORY_DIR)):
        company_symbol = file_name.split('_')[0]
        src_path = os.path.join('..', utils.DATA_DIR, utils.TRADING_HISTORY_DIR, file_name)
        dst_path = os.path.join('..', utils.DATA_DIR, SAVE_DIR, file_name)
        print('Update {}, {}'.format(company_symbol, src_path))
        df_old = pd.read_csv(src_path)
        # df_new = df_old.merge(get_latest_trading_data(company_symbol), how='outer')
        # df_new.to_csv(file_path)
