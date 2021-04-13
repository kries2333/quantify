import math
from datetime import datetime
import multiprocessing as mp
import pandas as pd
from Common.Common import log
from QAFetch.QAQuery import QA_fetch_cryptocurrency_min
from QAStrategy.Evaluate import equity_curve_for_OKEx_USDT_future_next_open
from QAStrategy.Position import position_for_OKEx_future
from QAStrategy.Signals import signal_simple_bolling, signal_simple_bolling_para_list
from QAUtil.Profile import profile

face_value = 0.01  # btc是0.01，不同的币种要进行不同的替换
c_rate = 5 / 10000  # 手续费，commission fees，默认为万分之5。不同市场手续费的收取方法不同，对结果有影响。比如和股票就不一样。
slippage = 1 / 1000  # 滑点 ，可以用百分比，也可以用固定值。建议币圈用百分比，股票用固定值
leverage_rate = 3
min_margin_ratio = 1 / 100  # 最低保证金率，低于就会爆仓
rule_type = '15T'
drop_days = 10  # 币种刚刚上线10天内不交易
df = pd.DataFrame

def start_strategy_booling_params(symbol, t, teb):
    pass


def train_on_parameter(param=[400, 2, 0.5]):
    _df = df.copy()

    signal_df = signal_simple_bolling(_df, param)
    position_df = position_for_OKEx_future(signal_df)
    equity_curve_df = equity_curve_for_OKEx_USDT_future_next_open(position_df)

    # 计算收益
    rtn = pd.DataFrame()
    rtn.loc[0, 'para'] = str(param)
    r = equity_curve_df.iloc[-1]['equity_curve']
    rtn.loc[0, 'equity_curve'] = r
    return rtn

if __name__ == "__main__":
    # for s in ['OKEX.ETH-USDT']:
    #     for t in ['5T', '15T', '30T', '60']:
    #         start_strategy_booling_params(s, t, 'normal')
    start_t = datetime.now()

    data = QA_fetch_cryptocurrency_min(
        code=[
            'OKEX.ETH-USDT'
        ],
        start='2017-10-01',
        end='2021-04-07',
        frequence='1min'
    )

    for t in ['60T']:
        period_df = data.resample(t, on='datetime', label='left', closed='left').agg(
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
        df = df[df['datetime'] >= pd.to_datetime('2017-10-01')]
        df.reset_index(inplace=True, drop=True)

        log(start_t, '数据库获取数据')

        params = signal_simple_bolling_para_list()

        num_cores = int(mp.cpu_count())
        print("本地计算机有: " + str(num_cores) + " 核心")
        pool = mp.Pool(num_cores)

        df_list = pool.map(train_on_parameter, params)
        para_curve_df = pd.concat(df_list, ignore_index=True)

        # =====输出
        para_curve_df.sort_values(by='equity_curve', ascending=False, inplace=True)
        print(para_curve_df)
        log(start_t, '完成')

        path = 'normal' + "_OKEX.ETH-USDT-" + t + '.csv'
        para_curve_df.to_csv(path)
