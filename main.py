import json
import os
import glob

from flask import Flask, request, jsonify
from flask_restful import Resource, Api
from crawler import utils

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
api = Api(app)


class MarketBreadth(Resource):
    def get(self, date):
        file_path = os.path.join(utils.DATA_DIR, utils.MARKET_BREADTH_DIR, '{}.json'.format(date))
        if not os.path.exists(file_path):
            response = jsonify({"ERROR": "File not found, {} is not a valid date. Please try another date.".format(date)})
        else:
            with open(file_path, 'r', encoding='utf-8') as json_file:
                market_breadth_json = json.load(json_file)
            # print(json.dumps(market_breadth_json, indent=4, ensure_ascii=False))
            response = jsonify(market_breadth_json)
            # Enable Access-Control-Allow-Origin
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response


api.add_resource(MarketBreadth, '/<int:date>')

if __name__ == '__main__':
    app.run(debug=True)
