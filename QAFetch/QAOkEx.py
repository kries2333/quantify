import requests
import json
import datetime
import time
from dateutil.tz import tzutc
import pandas as pd
import numpy as np
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from requests.exceptions import ConnectTimeout, SSLError, ReadTimeout, ConnectionError
# from retrying import retry
from urllib.parse import urljoin

from QAUitl.QADate_Adv import QA_util_datetime_to_Unix_timestamp

"""
OKEx 只允许一次获取 200bar，时间请求超过范围则只返回最新200条
"""

TIMEOUT = 10
OKEx_base_url = "https://www.okex.com/"

column_names = [
    'time',
    'open',
    'high',
    'low',
    'close',
    'volume',
]

"""
QUANTAXIS 和 okex 的 frequency 常量映射关系
"""
OKEx2QA_FREQUENCY_DICT = {
    "60": '1min',
    "300": '5min',
    "900": '15min',
    "1800": '30min',
    "3600": '60min',
    "86400": 'day',
}

FREQUENCY_SHIFTING = {
    "60": 11940,
    "300": 60000,
    "900": 180000,
    "1800": 360000,
    "3600": 720000,
    "86400": 17280000
}

def format_okex_data_fields(datas, symbol, frequency):
    """
        # 归一化数据字段，转换填充必须字段，删除多余字段
        参数名 	类型 	描述
        time 	String 	开始时间
        open 	String 	开盘价格
        high 	String 	最高价格
        low 	String 	最低价格
        close 	String 	收盘价格
        volume 	String 	交易量
        """
    frame = pd.DataFrame(datas, columns=column_names)
    frame['symbol'] = 'OKEX.{}'.format(symbol)
    # GMT+0 String 转换为 UTC Timestamp
    frame['time_stamp'] = pd.to_datetime(frame['time']
                                         ).astype(np.int64) // 10 ** 9
    # UTC时间转换为北京时间
    frame['datetime'] = pd.to_datetime(
        frame['time_stamp'], unit='s'
    ).dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai')
    frame['date'] = frame['datetime'].dt.strftime('%Y-%m-%d')
    frame['date_stamp'] = pd.to_datetime(
        frame['date']
    ).dt.tz_localize('Asia/Shanghai').astype(np.int64) // 10 ** 9
    frame['datetime'] = frame['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

    frame['created_at'] = int(
        time.mktime(datetime.datetime.now().utctimetuple())
    )
    frame['updated_at'] = int(
        time.mktime(datetime.datetime.now().utctimetuple())
    )
    frame.drop(['time'], axis=1, inplace=True)
    frame['trade'] = 1
    frame['amount'] = frame.apply(
        lambda x: float(x['volume']) *
                  (float(x['open']) + float(x['close'])) / 2,
        axis=1
    )
    if (frequency not in ['1day', 'day', '86400', '1d']):
        frame['type'] = OKEx2QA_FREQUENCY_DICT[frequency]
    return frame

def QA_fetch_okex_kline_min(
    symbol,
    start_time,
    end_time,
    frequency,
    callback_func=None
):
    """
    Get the latest symbol‘s candlestick data with time slices
    时间倒序切片获取算法，是各大交易所获取1min数据的神器，因为大部分交易所直接请求跨月跨年的1min分钟数据
    会直接返回空值，只有将 start_epoch，end_epoch 切片细分到 200/300 bar 以内，才能正确返回 kline，
    火币和binance，OKEx 均为如此，用上面那个函数的方式去直接请求上万bar 的分钟 kline 数据是不会有结果的。
    """
    reqParams = {}
    reqParams['from'] = end_time - FREQUENCY_SHIFTING[frequency]
    reqParams['to'] = end_time

    while (reqParams['to'] > start_time):
        if ((reqParams['from'] > QA_util_datetime_to_Unix_timestamp())) or \
                ((reqParams['from'] > reqParams['to'])):

            # 跳到下一个时间段
            reqParams['to'] = int(reqParams['from'] - 1)
            reqParams['from'] = int(reqParams['from'] - FREQUENCY_SHIFTING[frequency])
            continue

        klines = QA_fetch_okex_kline_with_auto_retry(
            symbol,
            reqParams['from'],
            reqParams['to'],
            frequency,
        )

        if (klines is None) or \
                (len(klines) == 0) or \
                ('error' in klines):
            # 出错放弃
            break

        reqParams['to'] = int(reqParams['from'] - 1)
        reqParams['from'] = int(reqParams['from'] - FREQUENCY_SHIFTING[frequency])

        if (callback_func is not None):
            frame = format_okex_data_fields(klines, symbol, frequency)
            callback_func(frame, OKEx2QA_FREQUENCY_DICT[frequency])

        if (len(klines) == 0):
            return None

# https://www.okex.com/api/spot/v3/instruments/BTC-USDT/history/candles?granularity=60&start=2020-03-29T08:13:00.000Z&end=2020-03-29T03:14:00.000Z
def QA_fetch_okex_kline_with_auto_retry(
    symbol,
    start_time,
    end_time,
    frequency,
):
    """
    Get the latest symbol‘s candlestick data raw method
    获取币对的K线历史数据。K线数据按请求的粒度分组返回，k线数据最多可获取200条(说明文档中2000条系错误)。
    限速规则：20次/2s
    HTTP请求 GET/api/spot/v3/instruments/<instrument_id>/history/candles
    """
    url = urljoin(
        OKEx_base_url,
        "/api/spot/v3/instruments/{:s}/history/candles".format(symbol)
    )

    retries = 1
    while (retries != 0):
        try:
            start_epoch = datetime.datetime.fromtimestamp(
                start_time,
                tz=tzutc()
            )
            end_epoch = datetime.datetime.fromtimestamp(
                end_time,
                tz=tzutc()
            )
            start_epoch = start_epoch.isoformat().replace("+00:00", ".000Z")
            end_epoch = end_epoch.isoformat().replace("+00:00", ".000Z")
            req = requests.get(
                url,
                params={
                    "granularity": frequency,
                    "start": end_epoch,  # Z结尾的ISO时间 String
                    "end": start_epoch  # Z结尾的ISO时间 String
                },
                timeout=TIMEOUT
            )
            # 防止频率过快
            time.sleep(0.5)
            retries = 0
        except (ConnectTimeout, ConnectionError, SSLError, ReadTimeout):
            retries = retries + 1
            if (retries % 6 == 0):
                print("")
            time.sleep(0.5)

        if (retries == 0):
            # 成功获取才处理数据，否则继续尝试连接
            msg_dict = json.loads(req.content)

            if ('error_code' in msg_dict):
                print('Error', msg_dict)
                return None

            return msg_dict

    return None