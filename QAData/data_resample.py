from datetime import time
import pandas as pd
from pandas.tseries.frequencies import to_offset
import numpy as np

def QA_data_cryptocurrency_min_resample(min_data, type_='5min'):
    """数字加密资产的分钟线采样成大周期


    分钟线采样成子级别的分钟线


    time+ OHLC==> resample
    Arguments:
        min {[type]} -- [description]
        raw_type {[type]} -- [description]
        new_type {[type]} -- [description]
    """

    CONVERSION = {
        'code': 'first',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'trade': 'sum',
        'vol': 'sum',
        'amount': 'sum'
    } if 'vol' in min_data.columns else {
        'code': 'first',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'trade': 'sum',
        'volume': 'sum',
        'amount': 'sum'
    }
    min_data = min_data.loc[:, list(CONVERSION.keys())]
    # 适配 pandas 1.0+，避免出现 FutureWarning:
    # 'loffset' in .resample() and in Grouper() is deprecated 提示
    data = min_data.resample(
        type_,
        offest='0min',
        closed='right',
    ).apply(CONVERSION).dropna()
    data.index = data.index + to_offset(type_)
    return data.assign(datetime=pd.to_datetime(data.index, utc=False)
                      ).set_index(['datetime',
                                   'code'])