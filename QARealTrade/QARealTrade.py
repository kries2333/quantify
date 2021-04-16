# 处理实盘

# 1: 账号配置信息
# 2: 获取账号信息及持仓信息
# apikey = "7a314674-2f50-4621-a023-4c5a77c7f971"
# secretkey = "4BED850944D5EC719B2C52CD932489EF"
# IP = "0"
# 备注名 = "kries"
# 权限 = "只读/交易"

# =====配置交易相关参数=====
# 更新需要交易的合约、策略参数、下单量等配置信息

# =交易所配置
from time import sleep

import ccxt
import pandas as pd

# =执行的时间间隔
from QARealTrade.QAConfig import exchange_timeout, long_sleep_time, short_sleep_time
from QARealTrade.QAFuntions import update_symbol_info, sleep_until_run_time, single_threading_get_data, \
    calculate_signal, send_dingding_msg, single_threading_place_order, update_order_info, dingding_report_every_loop

time_interval = '5m'  # 目前支持5m，15m，30m，1h，2h等。得okex支持的K线才行。最好不要低于5m

OKEX_CONFIG = {
    'apiKey': '7a314674-2f50-4621-a023-4c5a77c7f971',
    'secret': '4BED850944D5EC719B2C52CD932489EF',
    'password': 'Tt84521485',
    'timeout': exchange_timeout,
    'rateLimit': 10,
    # 'hostname': 'okex.me',  # 无法fq的时候启用
    'enableRateLimit': False
}

exchange = ccxt.okex(OKEX_CONFIG)

symbol_config = {
    'eth-usdt': {'instrument_id': 'ETH-USDT-210416',  # 合约代码，当更换合约的时候需要手工修改
                 'leverage': '3',  # 控制实际交易的杠杆倍数，在实际交易中可以自己修改。此处杠杆数，必须小于页面上的最大杠杆数限制
                 'strategy_name': 'real_signal_simple_bolling',  # 使用的策略的名称
                 'para': [590, 3.4]}  # 策略参数
}

max_len = 1000  # 设定最多收集多少根K线，okex不能超过1440根
symbol_candle_data = dict()  # 用于存储K线数据

# 初始化symbol_info，在每次循环开始时都初始化
symbol_info_columns = ['账户权益', '持仓方向', '持仓量', '持仓收益率', '持仓收益', '持仓均价', '当前价格', '最大杠杆']

# 账号持仓
def account_positions():
    # =获取持仓数据
    symbol_info = pd.DataFrame(index=symbol_config.keys(), columns=symbol_info_columns)  # 转化为dataframe
    # 更新账户信息symbol_info
    symbol_info = update_symbol_info(exchange, symbol_info, symbol_config)
    print('\nsymbol_info:\n', symbol_info, '\n')
    return symbol_info

def real_klink_min(symbol_info):
    # =并行获取所有币种最近数据
    exchange.timeout = 5000  # 即将获取最新数据，临时将timeout设置为1s，加快获取数据速度
    candle_num = 1000  # 只获取最近candle_num根K线数据，可以获得更快的速度

    # =获取策略执行时间，并sleep至该时间
    run_time = sleep_until_run_time(time_interval)

    # 获取数据
    recent_candle_data = single_threading_get_data(exchange, symbol_info, symbol_config, time_interval, run_time,
                                                   candle_num)
    # 将symbol_candle_data和最新获取的recent_candle_data数据合并
    for symbol in symbol_config.keys():
        df = recent_candle_data[symbol]
        # df = symbol_candle_data[symbol].append(_df, ignore_index=True)
        df.drop_duplicates(subset=['candle_begin_time_GMT8'], keep='last', inplace=True)
    #     df.sort_values(by='candle_begin_time_GMT8', inplace=True)  # 排序，理论上这步应该可以省略，加快速度
        df = df.iloc[-max_len:]  # 保持最大K线数量不会超过max_len个
        df.reset_index(drop=True, inplace=True)
        symbol_candle_data[symbol] = df


    # =计算每个币种的交易信号
    symbol_signal = calculate_signal(symbol_info, symbol_config, symbol_candle_data)
    print('本周期交易计划:', symbol_signal)

    # =下单
    exchange.timeout = exchange_timeout  # 下单时需要增加timeout的时间，将timout恢复正常
    symbol_order = pd.DataFrame()
    if symbol_signal:
        symbol_order = single_threading_place_order(exchange, symbol_info, symbol_config, symbol_signal)  # 单线程下单
        print('下单记录：\n', symbol_order)

        # 更新订单信息，查看是否完全成交
        sleep(short_sleep_time)  # 休息一段时间再更新订单信息
        symbol_order = update_order_info(exchange, symbol_config, symbol_order)
        print('更新下单记录：', '\n', symbol_order)

    # 重新更新账户信息symbol_info
    sleep(long_sleep_time)  # 休息一段时间再更新
    symbol_info = pd.DataFrame(index=symbol_config.keys(), columns=symbol_info_columns)
    symbol_info = update_symbol_info(exchange, symbol_info, symbol_config)
    print('\nsymbol_info:\n', symbol_info, '\n')

    # 发送钉钉
    # dingding_report_every_loop(symbol_info, symbol_signal, symbol_order, run_time, robot_id_secret)

    # 本次循环结束
    print('\n', '-' * 20, '本次循环结束，%f秒后进入下一次循环' % long_sleep_time, '-' * 20, '\n\n')
    sleep(long_sleep_time)


def real_trade_start():
    while True:
        try:
            symbol_info = account_positions()
            real_klink_min(symbol_info)
        except Exception as e:
            send_dingding_msg('系统出错，10s之后重新运行，出错原因：' + str(e))
            print(e)
            sleep(long_sleep_time)


if __name__ == '__main__':
    real_trade_start()


