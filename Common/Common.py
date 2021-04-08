import datetime


def log(start_t, text):
    end_t = datetime.datetime.now()
    elapsed_sec = (end_t - start_t).total_seconds()
    print("{} 多线程计算共消耗: {:.2f}".format(text, elapsed_sec) + " 秒")