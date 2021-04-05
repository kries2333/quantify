import datetime
import time
from dateutil.tz import tzutc
from dateutil.relativedelta import relativedelta
import pandas as pd

# from QAUtil import (
#     DATABASE,
#     QASETTING,
#     QA_util_log_info,
#     QA_util_log_expection,
#     QA_util_to_json_from_pandas
# )

from QAUtil.QADate_Adv import (
    QA_util_timestamp_to_str,
    QA_util_datetime_to_Unix_timestamp,
    QA_util_print_timestamp
)
from QAFetch.QAOkEx import (
    # QA_fetch_okex_symbols,
    # QA_fetch_okex_kline,
    QA_fetch_okex_kline_min,
    OKEx2QA_FREQUENCY_DICT
)

from QAUtil.QALogs import QA_util_log_info
from QAUtil.QASetting import DATABASE
from QAUtil.QATransform import QA_util_to_json_from_pandas

from QAUtil.QAcrypto import (
    # QA_util_save_raw_symbols,
    QA_util_find_missing_kline
)
# from QAFetch.QAQuery import (QA_fetch_cryptocurrency_list)

import pymongo

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
    col = DATABASE.cryptocurrency_min
    col.create_index(
        [
            ("symbol",
             pymongo.ASCENDING),
            ('time_stamp',
             pymongo.ASCENDING),
            ('date_stamp',
             pymongo.ASCENDING)
        ]
    )
    col.create_index(
        [
            ("symbol",
             pymongo.ASCENDING),
            ("type",
             pymongo.ASCENDING),
            ('time_stamp',
             pymongo.ASCENDING)
        ],
        unique=True
    )

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

    symbol_list['symbol'] = ['BTC-USDT', 'ETH-USDT', 'EOS-USDT']

    end = datetime.datetime.now(tzutc())
    QA_util_log_info(
        'Starting DOWNLOAD PROGRESS of min Klines from {:s}... '.format(OKEx_EXCHANGE),
        ui_log=ui_log,
        ui_progress=ui_progress
    )

    for index in range(len(symbol_list)):
        symbol_info = symbol_list.iloc[index]

        QA_util_log_info(
            'DOWNLOAD PROGRESS {} '
                .format(str(float(index / len(symbol_list) * 100))[0:4] + '%'),
            ui_log=ui_log,
            ui_progress=ui_progress
        )
        query_id = {
            "symbol": symbol_template.format(symbol_info['symbol']),
            'type': OKEx2QA_FREQUENCY_DICT[frequency]
        }
        ref = col.find(query_id).sort('time_stamp', -1)

        if (col.count_documents(query_id) > 0):
            start_stamp = ref.next()['time_stamp']
            start_time = datetime.datetime.fromtimestamp(
                start_stamp + 1,
                tz=tzutc()
            )
            QA_util_log_info(
                'UPDATE_SYMBOL "{}" Trying updating "{}" from {} to {}'.format(
                    symbol_template.format(symbol_info['symbol']),
                    OKEx2QA_FREQUENCY_DICT[frequency],
                    QA_util_timestamp_to_str(start_time),
                    QA_util_timestamp_to_str(end)
                ),
                ui_log=ui_log,
                ui_progress=ui_progress
            )

            # 查询到 Kline 缺漏，点抓取模式，按缺失的时间段精确请求K线数据
            missing_data_list = QA_util_find_missing_kline(
                symbol_template.format(symbol_info['symbol']),
                OKEx2QA_FREQUENCY_DICT[frequency],
            )[::-1]
        else:
            start_time = OKEx_MIN_DATE
            QA_util_log_info(
                'NEW_SYMBOL "{}" Trying downloading "{}" from {} to {}'.format(
                    symbol_template.format(symbol_info['symbol']),
                    OKEx2QA_FREQUENCY_DICT[frequency],
                    QA_util_timestamp_to_str(start_time),
                    QA_util_timestamp_to_str(end)
                ),
                ui_log=ui_log,
                ui_progress=ui_progress
            )
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
                if (reqParams['from'] >
                    (QA_util_datetime_to_Unix_timestamp() + 120)):
                    # 出现“未来”时间，一般是默认时区设置错误造成的
                    QA_util_log_info(
                        'A unexpected \'Future\' timestamp got, Please check self.missing_data_list_func param \'tzlocalize\' set. More info: {:s}@{:s} at {:s} but current time is {}'
                        .format(
                            symbol_template.format(symbol_info['symbol']),
                            frequency,
                            QA_util_print_timestamp(reqParams['from']),
                            QA_util_print_timestamp(
                                QA_util_datetime_to_Unix_timestamp()
                            )
                        )
                    )
                    # 跳到下一个时间段
                    continue

                QA_util_log_info(
                    'Fetch "{:s}" slices "{:s}" kline：{:s} to {:s}'.format(
                        symbol_template.format(symbol_info['symbol']),
                        OKEx2QA_FREQUENCY_DICT[frequency],
                        QA_util_timestamp_to_str(
                            missing_data_list[i][expected]
                        )[2:16],
                        QA_util_timestamp_to_str(
                            missing_data_list[i][between]
                        )[2:16]
                    )
                )
                data = QA_fetch_okex_kline_min(
                    symbol_info['symbol'],
                    start_time=reqParams['from'],
                    end_time=reqParams['to'],
                    frequency=frequency,
                    callback_func=QA_SU_save_data_okex_callback
                )
            if data is None:
                QA_util_log_info(
                    'SYMBOL "{}" from {} to {} has no MORE data'.format(
                        symbol_template.format(symbol_info['symbol']),
                        QA_util_timestamp_to_str(start_time),
                        QA_util_timestamp_to_str(end)
                    )
                )
                continue
        QA_util_log_info(
            'DOWNLOAD PROGRESS of min Klines from {:s} accomplished.'.format(OKEx_EXCHANGE),
            ui_log=ui_log,
            ui_progress=ui_progress
        )

def save_data_to_file(data, freq):
    start_time = data['date'][0]
    print(start_time)
    path = str(start_time)
    # save_csv(data, 'okex', path, 'BTC_USDT_1m', 'spot')

def QA_SU_save_data_okex_callback(data, freq):
    """
    异步获取数据回调用的 MongoDB 存储函数，okex返回数据也是时间倒序排列
    """
    symbol_template = OKEx_SYMBOL
    QA_util_log_info(
        'SYMBOL "{}" Recived "{}" from {} to {} in total {} klines'.format(
            data.iloc[0].symbol,
            freq,
            time.strftime(
                '%Y-%m-%d %H:%M:%S',
                time.localtime(data.iloc[-1].time_stamp)
            )[2:16],
            time.strftime(
                '%Y-%m-%d %H:%M:%S',
                time.localtime(data.iloc[0].time_stamp)
            )[2:16],
            len(data)
        )
    )
    if (freq not in ['1day', '86400', 'day', '1d']):
        col = DATABASE.cryptocurrency_min
        col.create_index(
            [
                ("symbol",
                 pymongo.ASCENDING),
                ('time_stamp',
                 pymongo.ASCENDING),
                ('date_stamp',
                 pymongo.ASCENDING)
            ]
        )
        col.create_index(
            [
                ("symbol",
                 pymongo.ASCENDING),
                ("type",
                 pymongo.ASCENDING),
                ('time_stamp',
                 pymongo.ASCENDING)
            ],
            unique=True
        )

        # 查询是否新 tick
        query_id = {
            "symbol": data.iloc[0].symbol,
            'type': data.iloc[0].type,
            'time_stamp': {
                '$in': data['time_stamp'].tolist()
            }
        }
        refcount = col.count_documents(query_id)
    else:
        col = DATABASE.cryptocurrency_day
        col.create_index(
            [
                ("symbol",
                 pymongo.ASCENDING),
                ("date_stamp",
                 pymongo.ASCENDING)
            ],
            unique=True
        )

        # 查询是否新 tick
        query_id = {
            "symbol": data.iloc[0].symbol,
            'date_stamp': {
                '$in': data['date_stamp'].tolist()
            }
        }
        refcount = col.count_documents(query_id)
    if refcount > 0:
        if (len(data) > 1):
            # 删掉重复数据
            col.delete_many(query_id)
            data = QA_util_to_json_from_pandas(data)
            col.insert_many(data)
        else:
            # 持续接收行情，更新记录
            data.drop('created_at', axis=1, inplace=True)
            data = QA_util_to_json_from_pandas(data)
            col.replace_one(query_id, data[0])
    else:
        # 新 tick，插入记录
        data = QA_util_to_json_from_pandas(data)
        col.insert_many(data)

def QA_SU_save_okex():
    QA_SU_save_okex_min('60')