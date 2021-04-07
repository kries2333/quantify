import datetime
import threading
import time
import pandas as pd
from zenlog import logging

def QA_util_time_stamp(time_):
    """
    explanation:
       转换日期时间的字符串为浮点数的时间戳

    params:
        * time_->
            含义: 日期时间
            类型: str
            参数支持: ['2018-01-01 00:00:00']

    return:
        time
    """
    if len(str(time_)) == 10:
        # yyyy-mm-dd格式
        return time.mktime(time.strptime(time_, '%Y-%m-%d'))
    elif len(str(time_)) == 16:
        # yyyy-mm-dd hh:mm格式
        return time.mktime(time.strptime(time_, '%Y-%m-%d %H:%M'))
    else:
        timestr = str(time_)[0:19]
        return time.mktime(time.strptime(timestr, '%Y-%m-%d %H:%M:%S'))


def QA_util_log_info(logs, ui_log=None, ui_progress=None, ui_progress_int_value=None):
    """
    explanation:
        QUANTAXIS INFO级别日志接口

    params:
        * logs ->:
            meaning: 日志信息
            type: null
            optional: [null]
        * ui_log ->:
            meaning:
            type: null
            optional: [null]
        * ui_progress ->:
            meaning:
            type: null
            optional: [null]
        * ui_progress_int_value ->:
            meaning:
            type: null
            optional: [null]

    return:
        None

    demonstrate:
        Not described

    output:
        Not described
    """

    """
    QUANTAXIS Log Module
    @yutiansut

    QA_util_log_x is under [QAStandard#0.0.2@602-x] Protocol
    """
    logging.warning(logs)

    # 给GUI使用，更新当前任务到日志和进度
    if ui_log is not None:
        if isinstance(logs, str):
            ui_log.emit(logs)
        if isinstance(logs, list):
            for iStr in logs:
                ui_log.emit(iStr)

    if ui_progress is not None and ui_progress_int_value is not None:
        ui_progress.emit(ui_progress_int_value)


def QA_util_to_datetime(time):
    """
    explanation:
        转换字符串格式的日期为datetime

    params:
        * time->
            含义: 日期
            类型: str
            参数支持: []

    return:
        datetime
    """
    if len(str(time)) == 10:
        _time = '{} 00:00:00'.format(time)
    elif len(str(time)) == 19:
        _time = str(time)
    else:
        QA_util_log_info('WRONG DATETIME FORMAT {}'.format(time))
    return datetime.datetime.strptime(_time, '%Y-%m-%d %H:%M:%S')