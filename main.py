import json
import os
import glob

import pandas as pd
from flask import Flask, request, jsonify
from flask_restful import Resource, Api
from crawler import utils

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
api = Api(app)


class MarketBreadth(Resource):
    def get(self, date):
        file_path = os.path.join('twse-market-breadth-backend', utils.DATA_DIR, utils.MARKET_BREADTH_DIR, '{}.json'.format(date))
        if not os.path.exists(file_path):
            response = jsonify(
                {"ERROR": "File not found, {} is not a valid date. Please try another date.".format(date)})
        else:
            with open(file_path, 'r', encoding='utf-8') as json_file:
                market_breadth_json = json.load(json_file)
            # print(json.dumps(market_breadth_json, indent=4, ensure_ascii=False))
            response = jsonify(market_breadth_json)
            # Enable Access-Control-Allow-Origin
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response


class IndexInfo(Resource):
    def get(self, date):
        index_df = pd.read_csv(glob.glob(os.path.join('twse-market-breadth-backend', utils.DATA_DIR, utils.INDEX_HISTORY_DIR, '*'))[-1])
        request_date = date
        date = '/'.join([date[:4], date[4:6], date[6:]])
        date = date.split('/')
        date[0] = str(int(date[0]) - 1911)
        date = '/'.join(date)
        res = index_df[index_df.date == date]
        if res.empty:
            response = jsonify(
                {"ERROR": "{}".format(request_date)})
        else:
            res = {"close": res.close.values[0], "change": res.change.values[0],
                   "pct_change": res['pct_change'].values[0]}
            response = jsonify(res)
            # Enable Access-Control-Allow-Origin
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response


class IndexMonth(Resource):
    def get(self):
        index_df = pd.read_csv(glob.glob(os.path.join('twse-market-breadth-backend', utils.DATA_DIR, utils.INDEX_HISTORY_DIR, '*'))[-1])
        res = index_df.close.iloc[-20:].tolist()
        date_list = index_df.date.iloc[-20:].tolist()
        for i in range(len(date_list)):
            date_list[i] = date_list[i].split('/')
            date_list[i][0] = str(int(date_list[i][0]) + 1911)
            date_list[i] = '/'.join(date_list[i])
        res = [{
            "收盤指數": res[i],
            "date": date_list[i]
        } for i in range(len(res))]
        response = jsonify(res)
        # Enable Access-Control-Allow-Origin
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response


api.add_resource(MarketBreadth, '/<int:date>')
api.add_resource(IndexInfo, '/index-info-<string:date>')
api.add_resource(IndexMonth, '/index-month')

if __name__ == '__main__':
    app.run(debug=True)
