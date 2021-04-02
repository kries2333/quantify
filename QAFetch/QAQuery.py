import datetime

import numpy
import pandas as pd
from pandas import DataFrame

from QAUtil.QADate import QA_util_time_stamp
from QAUtil.QASetting import DATABASE


def QA_fetch_cryptocurrency_list(
    market=None,
    collections=DATABASE.cryptocurrency_list
):
    '''
    获取数字资产列表
    '''
    if (market is None):
        cryptocurrency_list = pd.DataFrame(
            [item for item in collections.find({})]
        )
        if (len(cryptocurrency_list) > 0):
            return cryptocurrency_list.drop(
                '_id',
                axis=1,
                inplace=False
            ).set_index(
                'symbol',
                drop=False
            )
        else:
            return pd.DataFrame(
                columns=[
                    'symbol',
                    'name',
                    'market',
                    'state',
                    'category',
                    'base_currency',
                    'quote_currency',
                    'price_precision',
                    'desc'
                ]
            )
    else:
        cryptocurrency_list = pd.DataFrame(
            [item for item in collections.find({"market": market})]
        )
        if (len(cryptocurrency_list) > 0):
            return cryptocurrency_list.drop(
                '_id',
                axis=1,
                inplace=False
            ).set_index(
                'symbol',
                drop=False
            )
        else:
            return pd.DataFrame(
                columns=[
                    'symbol',
                    'name',
                    'market',
                    'state',
                    'category',
                    'base_currency',
                    'quote_currency',
                    'price_precision',
                    'desc'
                ]
            )

def QA_fetch_cryptocurrency_min(
    code,
    start,
    end,
    format='pd',
    frequence='1min',
    collections=DATABASE.cryptocurrency_spot_min
):
    '''
     '获取数字资产分钟线'
     '''
    if frequence in ['1min', '1m']:
        frequence = '1min'
    elif frequence in ['5min', '5m']:
        frequence = '5min'
    elif frequence in ['15min', '15m']:
        frequence = '15min'
    elif frequence in ['30min', '30m']:
        frequence = '30min'
    elif frequence in ['60min', '60m']:
        frequence = '60min'
    _data = []
    # code = QA_util_code_tolist(code, auto_fill=False)
    cursor = collections.find(
        {
            'symbol': {
                '$in': code
            },
            "time_stamp":
                {
                    "$gte": QA_util_time_stamp(start),
                    "$lte": QA_util_time_stamp(end)
                },
            'type': frequence
        },
        batch_size=10000
    )
    if format in ['dict', 'json']:
        return [data for data in cursor]
    for item in cursor:
        _data.append(
            [
                str(item['symbol']),
                float(item['open']) if (item['open'] is not None) else item['open'],
                float(item['high']) if (item['high'] is not None) else item['high'],
                float(item['low']) if (item['low'] is not None) else item['low'],
                float(item['close']) if (item['close'] is not None) else item['close'],
                float(item['volume']) if (item['volume'] is not None) else item['volume'],
                float(item['trade']) if (item['trade'] is not None) else item['trade'],
                float(item['amount']) if (item['amount'] is not None) else item['amount'],
                item['time_stamp'],
                item['date'],
                item['datetime'],
                item['type']
            ]
        )

    _data = DataFrame(
        _data,
        columns=[
            # 原抓取时候存入mongdb本来按照交易所叫法为'symbol'，但是考虑兼容DataStruct，读取的时候字段改名叫'code'，并非拼写错误。
            'code',  # symbol
            'open',
            'high',
            'low',
            'close',
            'volume',
            'trade',
            'amount',
            'time_stamp',
            'date',
            'datetime',
            'type'
        ]
    )
    _data = _data.assign(datetime=pd.to_datetime(_data['datetime'], utc=False)
                         ).drop_duplicates((['datetime', 'code'])).set_index(
        'datetime',
        drop=False
    )

    if format in ['numpy', 'np', 'n']:
        return numpy.asarray(_data)
    elif format in ['list', 'l', 'L']:
        return numpy.asarray(_data).tolist()
    elif format in ['P', 'p', 'pandas', 'pd']:
        return _data
