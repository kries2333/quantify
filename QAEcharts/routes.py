import time
from datetime import datetime
from pprint import pformat

from QAStrategy.Signals import signal_simple_bolling
from Trade import calcBolling
from echarts_data import get_echarts_html
from QAFetch.QAQuery import QA_fetch_cryptocurrency_min
from util import json_response
import numpy as np

def routes(app):

    @app.route('/')
    def index():
        print('index')
        symbol = 'BINANCE.ETHUSDT'
        t = '15T'

        data = QA_fetch_cryptocurrency_min(
            code=[
                symbol
            ],
            start='2017-01-01',
            end='2021-04-07',
            frequence='1min'
        )

        print("数据加载完成")

        all_data = data.copy()
        period_df = all_data.resample(t, on='datetime', label='left', closed='left').agg(
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
        all_data = period_df[['datetime', 'open', 'high', 'low', 'close', 'volume']]
        # all_data.reset_index(inplace=True, drop=True)

        df = all_data.copy()

        signal = '[],'

        # todo
        # 参考如下写法，替换自己的交易信号函数
        df = signal_simple_bolling(df, [580, 3.5])
        df['datetime'] = df['datetime'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        _df = df[['datetime', 'open', 'close', 'low', 'high']]
        _df_boll = df[['upper', 'lower', 'median', 'volume']]

        _df_boll['upper'].fillna(value=0, inplace=True)
        _df_boll['lower'].fillna(value=0, inplace=True)
        _df_boll['median'].fillna(value=0, inplace=True)
        # _df_boll['ema_short'].fillna(value=0, inplace=True)
        # _df_boll['ema_long'].fillna(value=0, inplace=True)
        _df_list = np.array(_df).tolist()
        _df_boll_list = np.array(_df_boll).transpose().tolist()
        str_df_list = pformat(_df_list)
        str_df_boll_list = pformat(_df_boll_list)

        if 'signal' in df.columns.tolist():
            x = list(df[df['signal'].notnull()]['datetime'])
            y = list(df[df['signal'].notnull()]['high'])
            z = list(df[df['signal'].notnull()]['signal'])
            signal = '['
            for i in zip(x, y, z):  # rgb(41,60,85)
                if i[2] == 1:
                    temp = "{coord:['" + str(i[0]) + "'," + str(i[
                                                                    1]) + "], label:{ normal: { formatter: function (param) { return \"买\";}} } ,itemStyle: {normal: {color: 'rgb(214,18,165)'}}},"
                elif i[2] == -1:
                    temp = "{coord:['" + str(i[0]) + "'," + str(
                        i[
                            1]) + "] , label:{ normal: { formatter: function (param) { return \"卖\";}} } ,itemStyle: {normal: {color: 'rgb(0,0,255)'}}},"
                else:
                    temp = "{coord:['" + str(i[0]) + "'," + str(
                        i[
                            1]) + "], label:{ normal: { formatter: function (param) { return \"平仓\";}} },itemStyle: {normal: {color: 'rgb(224,136,11)'}}},"

                signal += temp
            signal = signal.rstrip(',')
            signal += '],'

        _html = get_echarts_html(symbol,str_df_list,str_df_boll_list,signal)
        return _html

    @app.route("/query")
    def query_echarts():
        print("query_log")
