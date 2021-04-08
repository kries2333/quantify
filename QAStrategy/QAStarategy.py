import datetime
import multiprocessing as mp
import pandas as pd

from Common.Common import log
from QAFetch.QAQuery import QA_fetch_cryptocurrency_min
from QAStrategy.Evaluate import equity_curve_for_OKEx_USDT_future_next_open
from QAStrategy.Position import position_for_OKEx_future
from QAStrategy.Signals import signal_simple_bolling, signal_simple_bolling_para_list


def train_on_parameter(df, param, result_dict, result_lock):
    signal_df = signal_simple_bolling(df, param)
    position_df = position_for_OKEx_future(signal_df)
    _df = equity_curve_for_OKEx_USDT_future_next_open(position_df)
    r = _df.iloc[-1]['equity_curve']
    result_dict[str(param)] = r
    return


def start_strategy_booling_params(symbol, t, teb):
    start_t = datetime.datetime.now()

    num_cores = int(mp.cpu_count())
    print("本地计算机有: " + str(num_cores) + " 核心")
    pool = mp.Pool(num_cores)

    data = QA_fetch_cryptocurrency_min(
        code=[
            symbol
        ],
        start='2017-10-01',
        end='2021-04-07',
        frequence='1min'
    )

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

    manager = mp.Manager()
    managed_locker = manager.Lock()
    managed_dict = manager.dict()

    params = signal_simple_bolling_para_list()

    _df = df.copy()
    results = [pool.apply_async(train_on_parameter, args=(_df, param, managed_dict, managed_locker)) for param in params]
    results = [p.get() for p in results]

    log(start_t, '计算参数')

    _dict = sorted(managed_dict.items(), key=lambda d: d[1])
    out = pd.DataFrame(columns={
        'param',
        'equity_curve'
    })

    log(start_t, '参考值排序')

    for index in range(len(_dict)):
        out.loc[index] = [_dict[index][0], _dict[index][1]]

    out_file = teb + "_" + symbol + "_" + t + "_bolling_"  + ".csv"
    out.to_csv(out_file)

    log(start_t, '完成')

if __name__ == "__main__":
    # for symbol in ['OKEX.BTC-USDT', 'OKEX.ETH-USDT', 'OKEX.EOS-USDT']:
    #     for t in ['15T', '30T', '60T']:
    #         start_strategy_booling(symbol, t, 'mod')

    start_strategy_booling_params('OKEX.ETH-USDT', '15T', 'test')
