from Common.common import save_csv
from QAFetch.QAOkEx import QA_fetch_okex_kline_min
from QAUitl.QADate_Adv import QA_util_datetime_to_Unix_timestamp
import datetime
from dateutil.tz import tzutc
import pandas as pd

# OKEx的历史数据只提供2000个bar
from QAUitl.QALogs import QA_util_log_info

OKEx_MIN_DATE = datetime.datetime(2017, 10, 1, tzinfo=tzutc())
OKEx_EXCHANGE = 'OKEX'
OKEx_SYMBOL = 'OKEX.{}'

def QA_SU_save_okex_min(
    frequency='60',
    ui_log=None,
    ui_progress=None):
    """
    Save OKEx min kline 分钟线数据，统一转化字段保存数据为 crypto_asset_min
    """
    symbol_template = OKEx_SYMBOL
    symbol_list = pd.DataFrame(
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

    symbol_list['symbol'] = ['BTC-USDT']

    end = datetime.datetime.now(tzutc())
    QA_util_log_info(
        'Starting DOWNLOAD PROGRESS of min Klines from {:s}... '.format(OKEx_EXCHANGE),
        ui_log=ui_log,
        ui_progress=ui_progress
    )

    for index in range(len(symbol_list)):
        symbol_info = symbol_list.iloc[index]

        start_time = OKEx_MIN_DATE
        miss_kline = pd.DataFrame(
            [
                [
                    int(QA_util_datetime_to_Unix_timestamp(start_time)),
                    int(QA_util_datetime_to_Unix_timestamp(end)),
                    '{} to {}'.format(start_time,
                                      end)
                ]
            ],
            columns=['expected',
                     'between',
                     'missing']
        )
        missing_data_list = miss_kline.values

        if len(missing_data_list) > 0:
            # 查询确定中断的K线数据起止时间，缺分时数据，补分时数据
            expected = 0
            between = 1
            missing = 2
            reqParams = {}
            for i in range(len(missing_data_list)):
                reqParams['from'] = int(missing_data_list[i][expected])
                reqParams['to'] = int(missing_data_list[i][between])

                data = QA_fetch_okex_kline_min(
                    symbol_info['symbol'],
                    start_time=reqParams['from'],
                    end_time=reqParams['to'],
                    frequency=frequency,
                    callback_func=QA_SU_save_data_okex_callback
                )

def QA_SU_save_data_okex_callback(data, freq):
    """
    异步获取数据回调用的 MongoDB 存储函数，okex返回数据也是时间倒序排列
    """
    # print('QA_SU_save_data_okex_callback')
    start_time = data['date'][0]
    print(start_time)
    path = str(start_time)
    save_csv(data, 'okex', path, 'BTC_USDT_1m', 'spot')

def QA_SU_save_okex():
    QA_SU_save_okex_min('60')