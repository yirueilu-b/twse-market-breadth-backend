import os

import pandas as pd
from bs4 import BeautifulSoup
import requests

DATA_DIR = 'data'
FILE_NAME = 'company_list'
# url for english version
# URL = 'https://isin.twse.com.tw/isin/e_C_public.jsp?strMode=2'
URL = 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=2'


def get_company_df():
    """
    request data from `URL` to get the list of companies in TWSE.
    The data should contains the code and name of all companies
    :return: pd.DataFrame with 6 columns 'code', 'name', 'isin_code', 'date', 'category', 'cfi_code'
    """
    # get the whole table
    request = requests.get(URL)
    # parse
    soup = BeautifulSoup(request.text, 'html.parser')
    # split to rows
    rows = soup.findAll('tr')
    # remove header
    rows = rows[2:]
    # extract companies only, iterate through rows till find empty row
    # rules of the table may change, need to review in the future
    company_list = []
    for row in rows:
        row = [x.text for x in row.findAll('td')][:-1]
        if len(row) == 0: break
        row = row[0].split('\u3000') + row[1:3] + row[4:]
        company_list.append(row)
    company_df = pd.DataFrame(company_list, columns=['symbol', 'name', 'isin_code', 'date', 'category', 'cfi_code'])
    return company_df


if __name__ == '__main__':
    df = get_company_df()
    df.to_csv(os.path.join('..', 'data', 'company_list.csv'), index=False)
