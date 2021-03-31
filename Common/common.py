import re
import datetime
from calendar import calendar
import os


def parse8601(timestamp=None):
    if timestamp is None:
        return timestamp
    yyyy = '([0-9]{4})-?'
    mm = '([0-9]{2})-?'
    dd = '([0-9]{2})(?:T|[\\s])?'
    h = '([0-9]{2}):?'
    m = '([0-9]{2}):?'
    s = '([0-9]{2})'
    ms = '(\\.[0-9]{1,3})?'
    tz = '(?:(\\+|\\-)([0-9]{2})\\:?([0-9]{2})|Z)?'
    regex = r'' + yyyy + mm + dd + h + m + s + ms + tz
    try:
        match = re.search(regex, timestamp, re.IGNORECASE)
        if match is None:
            return None
        yyyy, mm, dd, h, m, s, ms, sign, hours, minutes = match.groups()
        ms = ms or '.000'
        ms = (ms + '00')[0:4]
        msint = int(ms[1:])
        sign = sign or ''
        sign = int(sign + '1') * -1
        hours = int(hours or 0) * sign
        minutes = int(minutes or 0) * sign
        offset = datetime.timedelta(hours=hours, minutes=minutes)
        string = yyyy + mm + dd + h + m + s + ms + 'Z'
        dt = datetime.datetime.strptime(string, "%Y%m%d%H%M%S.%fZ")
        dt = dt + offset
        return calendar.timegm(dt.utctimetuple()) * 1000 + msint
    except (TypeError, OverflowError, OSError, ValueError):
        return None

def save_csv(df, exchange_name, folder_name, file_name, type):
    # =====保存数据到文件
    path = './数据文件/data/history_kline'

    # 创建交易所文件
    path = os.path.join(path, exchange_name)
    if os.path.exists(path) is False:
        os.mkdir(path)

    # 创建spot文件夹
    path = os.path.join(path, type)
    if os.path.exists(path) is False:
        os.mkdir(path)

    # 创建日期文件夹
    path = os.path.join(path, folder_name)
    if os.path.exists(path) is False:
        os.mkdir(path)

    # 保存文件
    file_name = file_name + '.csv'
    path = os.path.join(path, file_name)

    df.to_csv(path, index=False)