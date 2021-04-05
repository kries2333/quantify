from datetime import datetime, timezone, timedelta
from pandas import np


def QA_util_datetime_to_Unix_timestamp(ts_epoch = None):
    """
    返回当前UTC时间戳，默认时区为北京时间
    :return: 类型 int
    """
    if (ts_epoch is None):
        ts_epoch = datetime.now(timezone(timedelta(hours=8)))

    return (ts_epoch - datetime(1970,1,1, tzinfo=timezone.utc)).total_seconds()

def QA_util_timestamp_to_str(ts_epoch = None, local_tz = timezone(timedelta(hours=8))):
    """
    返回字符串格式时间
    :return: 类型string
    """
    if (ts_epoch is None):
        ts_epoch = datetime.now(timezone(timedelta(hours=8)))

    if isinstance(ts_epoch, datetime):
        try:
            return ts_epoch.astimezone(local_tz).strftime('%Y-%m-%d %H:%M:%S')
        except:
            return ts_epoch.tz_localize(local_tz).strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(ts_epoch, int) or isinstance(ts_epoch, np.int32) or isinstance(ts_epoch, np.int64) or isinstance(ts_epoch, float):
        return (datetime(1970,1,1, tzinfo=timezone.utc) + timedelta(seconds = int(ts_epoch))).astimezone(local_tz).strftime('%Y-%m-%d %H:%M:%S')
    else:
        raise Exception('No support type %s.' % type(ts_epoch))

def QA_util_print_timestamp(ts_epoch):
    """
    打印合适阅读的时间格式
    """
    return '{:s}({:d})'.format(
        QA_util_timestamp_to_str(ts_epoch)[2:16],
        int(ts_epoch)
    )