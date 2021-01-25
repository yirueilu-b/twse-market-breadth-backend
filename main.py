import json
import os

from flask import Flask, request
from flask_restful import Resource, Api
from crawler import utils

app = Flask(__name__)
api = Api(app)


class MarketBreadth(Resource):
    def get(self, date):
        file_path = os.path.join(utils.DATA_DIR, utils.MARKET_BREADTH_DIR, '{}.json'.format(date))
        if not os.path.exists(file_path):
            return {"ERROR": "File not found, {} is not a valid date. Please try another date.".format(date)}

        with open(file_path, 'r') as json_file:
            market_breadth_json = json.load(json_file)
        # print(json.dumps(market_breadth_json, indent=4, ensure_ascii=False))
        return market_breadth_json


api.add_resource(MarketBreadth, '/<int:date>')

if __name__ == '__main__':
    app.run(debug=True)
