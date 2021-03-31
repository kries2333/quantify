from Common.common import save_csv
from QAFetch.QAOkEx import QA_fetch_okex_kline_min
from QAUitl.QADate_Adv import QA_util_datetime_to_Unix_timestamp
import datetime
from dateutil.tz import tzutc
import pandas as pd

def QA_SU_save_okex_min(
    frequency='60',
    ui_log=None,
    ui_progress=None):
    """
    Save OKEx min kline 分钟线数据，统一转化字段保存数据为 crypto_asset_min
    """
    reqParams = {}


    reqParams['from'] = int(QA_util_datetime_to_Unix_timestamp(datetime.datetime(2020, 7, 25, 0, 0, 0, tzinfo=tzutc())))
    reqParams['to'] = int(QA_util_datetime_to_Unix_timestamp(datetime.datetime(2020, 7, 25, 0, 1, 0, tzinfo=tzutc())))

    data = QA_fetch_okex_kline_min(
        'BTC-USDT',
        start_time=reqParams['from'],
        end_time=reqParams['to'],
        frequency=frequency,
        callback_func=QA_SU_save_data_okex_callback
    )

def QA_SU_save_okex_min

def QA_SU_save_data_okex_callback(data, freq):
    """
    异步获取数据回调用的 MongoDB 存储函数，okex返回数据也是时间倒序排列
    """
    # print('QA_SU_save_data_okex_callback')
    start_time = data['date'][0]
    print(start_time)
    path = str(start_time)
    save_csv(data, 'okex', path, 'BTC_USDT_1m', 'spot')

def Start():
    QA_SU_save_okex_min()