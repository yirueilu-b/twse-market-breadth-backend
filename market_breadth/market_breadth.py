import os
import json
import datetime

import pandas as pd
from crawler import utils


def calculate_percentage_above_ma(company_list, category, ma_window_size, date):
    """
    given company list, category and window size,
    1. group companies by category
    2. for each company in group
    3. calculate MA with window size and compare to close price
    4. get last comparison and append to array (the array is storing latest comparison of each company in category)
    5. calculate the percentage of True in array
    :param company_list: pd.DataFrame, all companies listed on TWSE
    :param category: str, the category of industry, could be extract from `category` column in `company list`
    :param ma_window_size: int, interval for calculating moving average
    :param date: the date in yyyy/mm/dd and is in
    :return: float, the percentage of companies whose close price are above MA
    """
    above_ma = []
    date = date.split('/')
    date[0] = str(int(date[0]) - 1911)
    date = '/'.join(date)
    grouped_company_list = company_list.groupby('category')
    companies = grouped_company_list.get_group(category)
    for index, company in companies.iterrows():
        symbol = company.symbol
        if symbol in utils.SKIP_SYMBOL:
            continue
        print(symbol, company['name'])
        """
        according to the symbol in `crawler_all_history.py`,
        the start date should be 201001 or the date when company listed on TWSE
        """
        twse_list_date = int(''.join(company.date.split('/')[:2]))
        if 201001 > twse_list_date:
            start = "201001"
        else:
            start = str(twse_list_date)
        """
        end must be the latest date, 
        and must update trading history data before running this function
        TODO: is regular expression could be better?
        """
        end = str(datetime.datetime.now().year) + str(datetime.datetime.now().month).zfill(2)
        file_name = "{}_{}_{}.csv".format(symbol, start, end)
        file_path = os.path.join('..', utils.DATA_DIR, utils.TRADING_HISTORY_DIR, file_name)
        try:
            df = pd.read_csv(file_path)
            df.date = df.date.apply(str.strip)
            # calculate ma and compare to close price
            close = pd.to_numeric(df.close, errors='coerce')
            close = close.fillna(method='ffill')
            is_above_ma = (close > close.rolling(window=ma_window_size).mean())
            target = df[df.date == date].index
            if target.any():
                above_ma.append(is_above_ma[target].values[0])
        except Exception as e:
            print("ERROR: Cannot open {}\n{}".format(file_path, e))
    if len(above_ma):
        percentage = above_ma.count(True) / len(above_ma) * 100
    else:
        percentage = -1.
    return percentage


def calculate_ma(close, ma_interval, target_index):
    ma_values = close.rolling(window=ma_interval).mean()
    above_ma_bool = (close > ma_values)
    if target_index.any():
        return above_ma_bool[target_index].values[0], ma_values[target_index].values[0]
    else:
        return None


def organize_trading_data_as_json(date, company_list):
    date = date.split('/')
    date[0] = str(int(date[0]) - 1911)
    date = '/'.join(date)

    industry_category = company_list.category.unique()
    ma_intervals = [5, 10, 20, 60]
    result = dict()
    """
    for each industry
    """
    for category in industry_category:
        grouped_company_list = company_list.groupby('category').get_group(category)
        summary = dict(zip(["ma{}".format(ma) for ma in ma_intervals], [[] for _ in range(len(ma_intervals))]))
        detail = []
        """ 
        for each company in industry
        """
        for index, company in grouped_company_list.iterrows():
            company_detail = dict()
            company_detail['symbol'] = company.symbol
            company_detail['name'] = company['name']
            company_detail['en_name'] = company['en_name']
            """
            read company trading data
            """
            twse_list_date = int(''.join(company.date.split('/')[:2]))
            if 201001 > twse_list_date:
                start = "201001"
            else:
                start = str(twse_list_date)
            end = str(datetime.datetime.now().year) + str(datetime.datetime.now().month).zfill(2)
            file_name = "{}_{}_{}.csv".format(company.symbol, start, end)
            file_path = os.path.join('..', utils.DATA_DIR, utils.TRADING_HISTORY_DIR, file_name)
            try:
                df = pd.read_csv(file_path)
                """
                target_index to select given date
                """
                target_index = df[df.date == date].index
                close = pd.to_numeric(df.close, errors='coerce')
                close = close.fillna(method='ffill')
                company_detail['close_price'] = close[target_index].values[0]
                company_detail['change'] = float(df.change[target_index].values[0])
                for ma in ma_intervals:
                    is_above, ma_value = calculate_ma(close, ma, target_index)
                    company_detail['ma{}'.format(ma)] = ma_value
                    summary['ma{}'.format(ma)].append(is_above)
            except Exception as e:
                for ma in ma_intervals:
                    company_detail['ma{}'.format(ma)] = -1
                    summary['ma{}'.format(ma)].append(None)
                # print(company.symbol)
            detail.append(company_detail)

        for k, v in summary.items():
            if None not in v and v:
                summary[k] = v.count(True) / len(v) * 100
            else:
                summary[k] = -1.

        result[category_ch_en_map[category]] = {"summary": summary, "detail": detail}
    return result


if __name__ == '__main__':
    for i in range(5, 20):
        d = '2021/01/{}'.format(str(i).zfill(2))
        print(d)
        company_list = pd.read_csv('../data/company_list.csv')
        en_company_list = pd.read_csv('../data/en_company_list.csv')
        company_list['en_name'] = en_company_list.name.apply(str.strip)
        with open('../data/category_ch_en_map.json') as json_file:
            category_ch_en_map = json.load(json_file)
        res = organize_trading_data_as_json(d, company_list)
        print(json.dumps(res, indent=4, ensure_ascii=False))
        save_path = os.path.join('..', utils.DATA_DIR, utils.MARKET_BREADTH_DIR, '{}.json'.format(''.join(d.split('/'))))
        with open(save_path, 'w') as f:
            f.write(json.dumps(res, indent=4))
