from QARealTrade.QARealTrade import *
from save_okex import QA_SU_save_okex

if __name__ == '__main__':
    QA_SU_save_okex()   # 保存现货数据
    # for symbol in ['OKEX.BTC-USDT', 'OKEX.ETH-USDT', 'OKEX.EOS-USDT']:
    #     for t in ['15T', '30T', '60T']:
    #         start_strategy_booling(symbol, t, 'mod')
    # real_trade_start()
