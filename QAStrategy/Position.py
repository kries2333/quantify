# 由交易信号产生实际持仓
def position_for_OKEx_future(tmp_df):
    """
    根据signal产生实际持仓。考虑各种不能买入卖出的情况。
    所有的交易都是发生在产生信号的K线的结束时
    :param df:
    :return:
    """

    # ===由signal计算出实际的每天持有仓位
    # 在产生signal的k线结束的时候，进行买入
    tmp_df['signal'].fillna(method='ffill', inplace=True)
    tmp_df['signal'].fillna(value=0, inplace=True)  # 将初始行数的signal补全为0
    tmp_df['pos'] = tmp_df['signal'].shift()
    tmp_df['pos'].fillna(value=0, inplace=True)  # 将初始行数的pos补全为0

    # ===对无法买卖的时候做出相关处理
    # 例如：下午4点清算，无法交易；股票、期货当天涨跌停的时候无法买入；股票的t+1交易制度等等。
    # 当前周期持仓无法变动的K线
    condition = (tmp_df['datetime'].dt.hour == 16) & (tmp_df['datetime'].dt.minute == 0)
    tmp_df.loc[condition, 'pos'] = None
    # pos为空的时，不能买卖，只能和前一周期保持一致。
    tmp_df['pos'].fillna(method='ffill', inplace=True)

    # 在实际操作中，不一定会直接跳过4点这个周期，而是会停止N分钟下单。此时可以注释掉上面的代码。

    # ===将数据存入hdf文件中
    # 删除无关中间变量
    tmp_df.drop(['signal'], axis=1, inplace=True)

    return tmp_df