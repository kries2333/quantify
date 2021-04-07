from QAData.QADataStruct import QA_DataStruct_CryptoCurrency_min
from QAFetch.QAQuery import QA_fetch_cryptocurrency_min
from QAUtil.QASetting import DATABASE
import pandas as pd

def QA_fetch_cryptocurrency_min_adv(
    code,
    start,
    end=None,
    frequence='1min',
    if_drop_index=True,
    collections=DATABASE.cryptocurrency_min
):
    '''
     '获取数字加密资产分钟线'
     :param symbol:
     :param start:
     :param end:
     :param frequence:
     :param if_drop_index:
     :param collections:
     :return:
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

    # __data = [] 没有使用

    end = start if end is None else end
    if len(start) == 10:
        start = '{} 00:00:00'.format(start)
    if len(end) == 10:
        end = '{} 23:59:59'.format(end)

    res = QA_fetch_cryptocurrency_min(
        code,
        start,
        end,
        format='pd',
        frequence=frequence, collections=collections
    )
    if res is None:
        print(
            "QA Error QA_fetch_cryptocurrency_min_adv parameter symbol=%s start=%s end=%s frequence=%s call QA_fetch_cryptocurrency_min return None"
            % (code,
               start,
               end,
               frequence)
        )
    else:
        res_reset_index = res.set_index(
            ['datetime',
             'code'],
            drop=if_drop_index
        )
        # if res_reset_index is None:
        #     print("QA Error QA_fetch_cryptocurrency_min_adv set index 'date, code' return None")
        return QA_DataStruct_CryptoCurrency_min(res_reset_index)

if __name__ == "__main__":
    data = QA_fetch_cryptocurrency_min(
        code=[
            'OKEX.BTC-USDT'
        ],
        start='2021-01-01',
        end='2021-03-31',
        frequence='1min'
    )

    period_df = data.resample("5T", on='datetime', label='left', closed='left').agg(
        {'open': 'first',
         'high': 'max',
         'low': 'min',
         'close': 'last',
         'volume': 'sum',
         }
    )
    period_df.dropna(subset=['open'], inplace=True)  # 去除一天都没有交易的周期
    period_df = period_df[period_df['volume'] > 0]  # 去除成交量为0的交易周期
    period_df.reset_index(inplace=True)
    df = period_df[['datetime', 'open', 'high', 'low', 'close', 'volume']]
    df = df[df['datetime'] >= pd.to_datetime('2017-01-01')]
    df.reset_index(inplace=True, drop=True)

