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
    collections=DATABASE.cryptocurrency_spot_min
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
     })
    period_df.dropna(subset=['open'], inplace=True)  # 去除一天都没有交易的周期
    period_df = period_df[period_df['volume'] > 0]  # 去除成交量为0的交易周期
    period_df.reset_index(inplace=True)
    df = period_df[['datetime', 'open', 'high', 'low', 'close', 'volume']]
    df = df[df['datetime'] >= pd.to_datetime('2017-01-01')]
    df.reset_index(inplace=True, drop=True)

    # ==计算指标
    n = 400
    m = 2
    # 计算均线
    df['median'] = df['close'].rolling(n, min_periods=1).mean()
    # 计算上轨、下轨道
    df['std'] = df['close'].rolling(n, min_periods=1).std(ddof=0)  # ddof代表标准差自由度
    df['upper'] = df['median'] + m * df['std']
    df['lower'] = df['median'] - m * df['std']

    print(df)
    exit()

    # ==计算信号
    # 找出做多信号
    condition1 = df['close'] > df['upper']  # 当前K线的收盘价 > 上轨
    condition2 = df['close'].shift(1) <= df['upper'].shift(1)  # 之前K线的收盘价 <= 上轨
    df.loc[condition1 & condition2, 'signal_long'] = 1  # 将产生做多信号的那根K线的signal设置为1，1代表做多

    # 找出做多平仓信号
    condition1 = df['close'] < df['median']  # 当前K线的收盘价 < 中轨
    condition2 = df['close'].shift(1) >= df['median'].shift(1)  # 之前K线的收盘价 >= 中轨
    df.loc[condition1 & condition2, 'signal_long'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

    # 找出做空信号
    condition1 = df['close'] < df['lower']  # 当前K线的收盘价 < 下轨
    condition2 = df['close'].shift(1) >= df['lower'].shift(1)  # 之前K线的收盘价 >= 下轨
    df.loc[condition1 & condition2, 'signal_short'] = -1  # 将产生做空信号的那根K线的signal设置为-1，-1代表做空

    # 找出做空平仓信号
    condition1 = df['close'] > df['median']  # 当前K线的收盘价 > 中轨
    condition2 = df['close'].shift(1) <= df['median'].shift(1)  # 之前K线的收盘价 <= 中轨
    df.loc[condition1 & condition2, 'signal_short'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

    # 合并做多做空信号，去除重复信号
    df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1, min_count=1, skipna=True)
    temp = df[df['signal'].notnull()][['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]
    df['signal'] = temp['signal']

    print(df)