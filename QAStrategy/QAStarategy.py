import pandas as pd

from QAFetch.QAQuery import QA_fetch_cryptocurrency_min
from QAStrategy.Evaluate import equity_curve_for_OKEx_USDT_future_next_open
from QAStrategy.Position import position_for_OKEx_future
from QAStrategy.Signals import signal_simple_bolling

if __name__ == "__main__":
    data = QA_fetch_cryptocurrency_min(
        code=[
            # 'OKEX.BTC-USDT',
            'OKEX.ETH-USDT',
            # 'OKEX.EOS-USDT'
        ],
        start='2019-01-01',
        end='2021-03-31',
        frequence='1min'
    )

    period_df = data.resample("60T", on='datetime', label='left', closed='left').agg(
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
    df = df[df['datetime'] >= pd.to_datetime('2020-01-01')]
    df.reset_index(inplace=True, drop=True)

    signal_df = signal_simple_bolling(df)
    position_df = position_for_OKEx_future(signal_df)
    res = equity_curve_for_OKEx_USDT_future_next_open(position_df)
    print(res)