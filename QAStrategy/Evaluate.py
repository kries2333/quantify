import pandas as pd
import numpy as np

# =====计算资金曲线
# okex交割合约（usdt本位）资金曲线
from QAUtil.Profile import profile


# @profile
# def equity_curve_for_OKEx_USDT_future_next_open(df, slippage=1 / 1000, c_rate=5 / 10000, leverage_rate=3, face_value=0.01,
#                                                 min_margin_ratio=1/100):
#     """
#     okex交割合约（usdt本位）资金曲线
#     开仓价格是下根K线的开盘价，可以是其他的形式
#     相比之前杠杆交易的资金曲线函数，逻辑简单很多：手续费的处理、爆仓的处理等。
#     在策略中增加滑点的。滑点的处理和手续费是不同的。
#     :param df:
#     :param slippage:  滑点 ，可以用百分比，也可以用固定值。建议币圈用百分比，股票用固定值
#     :param c_rate:  手续费，commission fees，默认为万分之5。不同市场手续费的收取方法不同，对结果有影响。比如和股票就不一样。
#     :param leverage_rate:  杠杆倍数
#     :param face_value:  一张合约的面值，0.01BTC
#     :param min_margin_ratio: 最低保证金率，低于就会爆仓
#     :return:
#     """
#     # =====下根k线开盘价
#     df['next_open'] = df['open'].shift(-1)  # 下根K线的开盘价
#     df['next_open'].fillna(value=df['close'], inplace=True)
#
#     # =====找出开仓、平仓的k线
#     condition1 = df['pos'] != 0  # 当前周期不为空仓
#     condition2 = df['pos'] != df['pos'].shift(1)  # 当前周期和上个周期持仓方向不一样。
#     open_pos_condition = condition1 & condition2
#
#     condition1 = df['pos'] != 0  # 当前周期不为空仓
#     condition2 = df['pos'] != df['pos'].shift(-1)  # 当前周期和下个周期持仓方向不一样。
#     close_pos_condition = condition1 & condition2
#
#     # =====对每次交易进行分组
#     df.loc[open_pos_condition, 'start_time'] = df['datetime']
#     df['start_time'].fillna(method='ffill', inplace=True)
#     df.loc[df['pos'] == 0, 'start_time'] = pd.NaT
#
#     # =====开始计算资金曲线
#     initial_cash = 10000  # 初始资金，默认为10000元
#     # ===在开仓时
#     # 在open_pos_condition的K线，以开盘价计算买入合约的数量。（当资金量大的时候，可以用5分钟均价）
#     df.loc[open_pos_condition, 'contract_num'] = initial_cash * leverage_rate / (face_value * df['open'])
#     df['contract_num'] = np.floor(df['contract_num'])  # 对合约张数向下取整
#     # 开仓价格：理论开盘价加上相应滑点
#     df.loc[open_pos_condition, 'open_pos_price'] = df['open'] * (1 + slippage * df['pos'])
#     # 开仓之后剩余的钱，扣除手续费
#     df['cash'] = initial_cash - df['open_pos_price'] * face_value * df['contract_num'] * c_rate  # 即保证金
#
#     # ===开仓之后每根K线结束时
#     # 买入之后cash，contract_num，open_pos_price不再发生变动
#     for _ in ['contract_num', 'open_pos_price', 'cash']:
#         df[_].fillna(method='ffill', inplace=True)
#     df.loc[df['pos'] == 0, ['contract_num', 'open_pos_price', 'cash']] = None
#
#     # ===在平仓时
#     # 平仓价格
#     df.loc[close_pos_condition, 'close_pos_price'] = df['next_open'] * (1 - slippage * df['pos'])
#     # 平仓之后剩余的钱，扣除手续费
#     df.loc[close_pos_condition, 'close_pos_fee'] = df['close_pos_price'] * face_value * df['contract_num'] * c_rate
#
#     # ===计算利润
#     # 开仓至今持仓盈亏
#     df['profit'] = face_value * df['contract_num'] * (df['close'] - df['open_pos_price']) * df['pos']
#     # 平仓时理论额外处理
#     df.loc[close_pos_condition, 'profit'] = face_value * df['contract_num'] * (
#             df['close_pos_price'] - df['open_pos_price']) * df['pos']
#     # 账户净值
#     df['net_value'] = df['cash'] + df['profit']
#
#     # ===计算爆仓
#     # 至今持仓盈亏最小值
#     df.loc[df['pos'] == 1, 'price_min'] = df['low']
#     df.loc[df['pos'] == -1, 'price_min'] = df['high']
#     df['profit_min'] = face_value * df['contract_num'] * (df['price_min'] - df['open_pos_price']) * df['pos']
#     # 账户净值最小值
#     df['net_value_min'] = df['cash'] + df['profit_min']
#     # 计算保证金率
#     df['margin_ratio'] = df['net_value_min'] / (face_value * df['contract_num'] * df['price_min'])
#     # 计算是否爆仓
#     df.loc[df['margin_ratio'] <= (min_margin_ratio + c_rate), '是否爆仓'] = 1
#
#     # ===平仓时扣除手续费
#     df.loc[close_pos_condition, 'net_value'] -= df['close_pos_fee']
#     # 应对偶然情况：下一根K线开盘价格价格突变，在平仓的时候爆仓。此处处理有省略，不够精确。
#     df.loc[close_pos_condition & (df['net_value'] < 0), '是否爆仓'] = 1
#
#     # ===对爆仓进行处理
#     df['是否爆仓'] = df.groupby('start_time')['是否爆仓'].fillna(method='ffill')
#     df.loc[df['是否爆仓'] == 1, 'net_value'] = 0
#
#     # =====计算资金曲线
#     df['equity_change'] = df['net_value'].pct_change()
#     df.loc[open_pos_condition, 'equity_change'] = df.loc[open_pos_condition, 'net_value'] / initial_cash - 1  # 开仓日的收益率
#     df['equity_change'].fillna(value=0, inplace=True)
#     df['equity_curve'] = (1 + df['equity_change']).cumprod()
#
#     # =====删除不必要的数据，并存储
#     df.drop(['next_open', 'contract_num', 'open_pos_price', 'cash', 'close_pos_price', 'close_pos_fee',
#              'profit', 'net_value', 'price_min', 'profit_min', 'net_value_min', 'margin_ratio', '是否爆仓'],
#             axis=1, inplace=True)
#
#     return df

def equity_curve_for_OKEx_USDT_future_next_open(df, initial_cash=10000, slippage=0.01, c_rate=0.0005, leverage_rate=3, face_value=0.01, min_margin_ratio=0.01):
    """
    okex交割合约（usdt本位）资金曲线
    开仓价格是下根K线的开盘价，可以是其他的形式
    相比之前杠杆交易的资金曲线函数，逻辑简单很多：手续费的处理、爆仓的处理等。
    在策略中增加滑点。滑点的处理和手续费是不同的。
    :param df:
    :type df: pd.DataFrame
    :param initial_cash: 初始资金
    :type initial_cash: int
    :param slippage: 滑点，可以用百分比，也可以用固定值。建议币圈用百分比，股票用固定值
    :type slippage: float
    :param c_rate: 手续费，commission fees，默认为万分之5。不同市场手续费的收取方法不同，对结果有影响。比如和股票就不一样。
    :type c_rate: float
    :param leverage_rate: 杠杆倍数
    :type leverage_rate: float
    :param face_value: 一张合约的面值
    :type face_value: float
    :param min_margin_ratio: 最低保证金率，低于就会爆仓
    :type min_margin_ratio: float
    :return:
    :rtype: pd.DataFrame
    """
    # =====下根k线开盘价
    next_open = df['open'].shift(-1).fillna(value=df['close']).values  # 下根K线的开盘价

    pos = df['pos'].values
    pos_condition = pos != 0  # 当前周期不为空仓
    # =====找出开仓、平仓的k线
    open_condition = df['pos'] != df['pos'].shift(1)  # 当前周期和上个周期持仓方向不一样。
    open_pos_condition = pos_condition & open_condition

    close_condition = df['pos'] != df['pos'].shift(-1)  # 当前周期和下个周期持仓方向不一样。
    close_pos_condition = pos_condition & close_condition

    # =====开始计算资金曲线
    # ===在开仓时
    # 在open_pos_condition的K线，以开盘价计算买入合约的数量。（当资金量大的时候，可以用5分钟均价）
    contract_num = np.where(open_pos_condition, np.floor((initial_cash * leverage_rate) / (face_value * df['open'].values)), np.nan)  # 对合约张数向下取整
    # 开仓价格：理论开盘价加上相应滑点
    open_pos_price = np.where(open_pos_condition, df['open'].values * (1 + slippage * pos), np.nan)
    # 开仓之后剩余的钱，扣除手续费
    cash = initial_cash - open_pos_price * contract_num * face_value * c_rate  # 即保证金

    # ===开仓之后每根K线结束时
    # 买入之后cash，contract_num，open_pos_price不再发生变动
    contract_num = np.where(pos_condition, pd.Series(contract_num).fillna(method='ffill').values, np.nan)
    open_pos_price = np.where(pos_condition, pd.Series(open_pos_price).fillna(method='ffill').values, np.nan)
    cash = np.where(pos_condition, pd.Series(cash).fillna(method='ffill').values, np.nan)

    # ===在平仓时
    # 平仓价格
    close_pos_price = np.where(close_pos_condition, next_open * (1 - slippage * pos), np.nan)
    # 平仓之后剩余的钱，扣除手续费
    close_pos_fee = np.where(close_pos_condition, close_pos_price * face_value * contract_num * c_rate, np.nan)

    # ===计算利润
    # 开仓至今持仓盈亏
    # 平仓时利润额外处理
    profit = np.where(close_pos_condition, face_value * contract_num * (close_pos_price - open_pos_price) * pos, face_value * contract_num * (df['close'].values - open_pos_price) * pos)
    # 账户净值
    net_value = cash + profit

    # ===计算爆仓
    # 至今持仓盈亏最小值
    price_min = np.where(pos == -1, df['high'].values, np.where(pos == 1, df['low'].values, np.nan))
    profit_min = face_value * contract_num * (price_min - open_pos_price) * pos
    # 账户净值最小值
    net_value_min = cash + profit_min
    # 计算保证金率
    margin_ratio = net_value_min / (face_value * contract_num * price_min)
    # 计算是否爆仓
    liquidation = np.where(margin_ratio <= (min_margin_ratio + c_rate), 1, np.nan)

    # ===平仓时扣除手续费
    net_value -= np.where(close_pos_condition, close_pos_fee, 0)
    # 应对偶然情况：下一根K线开盘价格价格突变，在平仓的时候爆仓。此处处理有省略，不够精确。
    liquidation = np.where(close_pos_condition & (net_value < 0), 1, liquidation)

    # ===对爆仓进行处理
    liquidation = pd.Series(liquidation).fillna(method='ffill').values
    net_value = np.where(liquidation == 1, 0, net_value)

    # =====计算资金曲线
    df['equity_change'] = np.where(open_pos_condition, net_value / initial_cash - 1, pd.Series(net_value).pct_change().values)  # 开仓日的收益率
    df['equity_change'].fillna(value=0, inplace=True)
    df['equity_curve'] = (1 + df['equity_change']).values.cumprod()

    return df
