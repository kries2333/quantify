from QARealTrade.QARealTrade import *
from QAStatistics.QAStatistics import QAStatistics_Start
from QAStrategy.QAStarategy import start_strategy_booling_params
from save_okex import QA_SU_save_okex

if __name__ == '__main__':
    # QA_SU_save_okex()   # 保存现货数据
    # for symbol in ['OKEX.BTC-USDT', 'OKEX.ETH-USDT', 'OKEX.EOS-USDT']:
    #     for t in ['5T', '15T', '30T', '60T']:
    #         start_strategy_booling_params(symbol, t, 'test')

    # QAStatistics_Start('OKEX.ETH-USDT', '60T', [80, 0.7, 0.9])
    # print("==================================================")
    real_trade_start()