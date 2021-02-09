import os
import json
import datetime
import math
import glob

import pandas as pd
from crawler import utils

TRADING_HISTORY_DIR = os.path.split(glob.glob(os.path.join('..',
                                                           utils.DATA_DIR, utils.TRADING_HISTORY_DIR + '*'))[-1])[-1]
all_trading_history_path = os.listdir(os.path.join('..', utils.DATA_DIR, TRADING_HISTORY_DIR))
symbol_path_map = dict(zip([int(x.split('_')[0]) for x in all_trading_history_path], all_trading_history_path))


def calculate_ma(close, ma_interval, target_index):
    ma_values = close.rolling(window=ma_interval).mean()
    above_ma_bool = (close > ma_values)
    if target_index.any():
        return above_ma_bool[target_index].values[0], ma_values[target_index].values[0]
    else:
        return None


def organize_trading_data_as_json(date, company_list):
    """
    for each industry:
        find the symbols in industry
        for each symbols:
            read the corresponding file
            calculate moving average
            compare the MA and close on given date

    """
    """
    industry_summary = {ma5:[res, res, ...], ma10:[], ...}
    res is the comparison result of MA and close on given date for symbol i
    """
    """
    industry_detail = [{symbol:, name:, ...}, {}]
    """
    date = date.split('/')
    date[0] = str(int(date[0]) - 1911)
    date = '/'.join(date)

    industry_category = company_list.category.unique()
    ma_intervals = [5, 10, 20, 60]
    result = dict()
    index_summary = dict(zip(["ma{}".format(ma) for ma in ma_intervals], [[] for _ in range(len(ma_intervals))]))

    for category in industry_category:
        grouped_company_list = company_list.groupby('category').get_group(category)
        industry_summary = dict(zip(["ma{}".format(ma) for ma in ma_intervals], [[] for _ in range(len(ma_intervals))]))
        industry_detail = []
        for index, company in grouped_company_list.iterrows():
            company_detail = dict()
            if company.symbol not in symbol_path_map:
                continue
            file_name = symbol_path_map[company.symbol]
            file_path = os.path.join('..', utils.DATA_DIR, TRADING_HISTORY_DIR, file_name)
            # print(file_path)
            df = pd.read_csv(file_path)
            df.date = df.date.apply(str.strip)
            target_index = df[df.date == date].index
            if len(target_index) == 0:
                continue
            close = pd.to_numeric(df.close, errors='coerce')
            close = close.fillna(method='ffill')

            for ma in ma_intervals:
                is_above, ma_value = calculate_ma(close, ma, target_index)
                if math.isnan(ma_value):
                    company_detail['ma{}'.format(ma)] = -1
                    continue
                else:
                    industry_summary['ma{}'.format(ma)].append(is_above)
                    index_summary['ma{}'.format(ma)].append(is_above)
                    company_detail['ma{}'.format(ma)] = ma_value

            company_detail['symbol'] = company.symbol
            company_detail['name'] = company['name']
            company_detail['en_name'] = company['en_name']
            company_detail['close_price'] = close[target_index].values[0]
            if df.change[target_index].values[0] == 'X0.00':
                pass
            else:
                company_detail['change'] = float(df.change[target_index].values[0])
            industry_detail.append(company_detail)

        for ma in ma_intervals:
            industry_summary["ma{}_count".format(ma)] = len(industry_summary["ma{}".format(ma)])
        for k, v in industry_summary.items():
            if 'count' not in k:
                if v:
                    industry_summary[k] = v.count(True) / len(v) * 100
                else:
                    industry_summary[k] = -1

        result[category] = {"summary": industry_summary, "detail": industry_detail}
        print(category, industry_summary)

    for ma in ma_intervals:
        index_summary["ma{}_count".format(ma)] = len(index_summary["ma{}".format(ma)])
    for k, v in index_summary.items():
        if 'count' not in k:
            if v:
                index_summary[k] = v.count(True) / len(v) * 100
            else:
                index_summary[k] = -1

    print(index_summary)
    result['大盤'] = {"summary": index_summary}

    return result


if __name__ == '__main__':
    company_list = pd.read_csv('../data/company_list.csv')
    en_company_list = pd.read_csv('../data/en_company_list.csv')
    company_list['en_name'] = en_company_list.name.apply(str.strip)
    with open('../data/category_ch_en_map.json') as json_file:
        category_ch_en_map = json.load(json_file)
    for month in [str(datetime.datetime.now().month - 1).zfill(2), str(datetime.datetime.now().month).zfill(2)]:
        for day in range(1, datetime.datetime.now().day):
            d = '2021/{}/{}'.format(month, str(day).zfill(2))
            print("Generate JSON Data for {}".format(d))
            res = organize_trading_data_as_json(d, company_list)
            # print(json.dumps(res, indent=4, ensure_ascii=False))
            save_path = os.path.join('..', utils.DATA_DIR, utils.MARKET_BREADTH_DIR, '{}.json'.format(''.join(d.split('/'))))
            with open(save_path, 'w') as f:
                f.write(json.dumps(res, indent=4))
