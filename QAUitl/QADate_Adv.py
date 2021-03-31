from datetime import datetime, timezone, timedelta

def QA_util_datetime_to_Unix_timestamp(ts_epoch = None):
    """
    返回当前UTC时间戳，默认时区为北京时间
    :return: 类型 int
    """
    if (ts_epoch is None):
        ts_epoch = datetime.now(timezone(timedelta(hours=8)))

    return (ts_epoch - datetime(1970,1,1, tzinfo=timezone.utc)).total_seconds()