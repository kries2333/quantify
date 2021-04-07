import configparser
import json
import os
from multiprocessing import Lock
from QASetting.QALocalize import qa_path, setting_path, strategy_path
from QAUtil.QASql import QA_util_sql_mongo_setting, QA_util_sql_async_mongo_setting

DEFAULT_MONGO = os.getenv('MONGODB', 'localhost')
DEFAULT_DB_URI = 'mongodb://{}:27017'.format(DEFAULT_MONGO)
CONFIGFILE_PATH = '{}{}{}'.format(setting_path, os.sep, 'config.ini')
INFO_IP_FILE_PATH = '{}{}{}'.format(setting_path, os.sep, 'info_ip.json')
STOCK_IP_FILE_PATH = '{}{}{}'.format(setting_path, os.sep, 'stock_ip.json')
FUTURE_IP_FILE_PATH = '{}{}{}'.format(setting_path, os.sep, 'future_ip.json')

class QA_Setting():

    def __init__(self, uri=None):
        self.lock = Lock()

        self.mongo_uri = uri or self.get_mongo()
        self.username = 'root'
        self.password = '123456'

        # 加入配置文件地址

    def get_mongo(self):
        config = configparser.ConfigParser()
        if os.path.exists(CONFIGFILE_PATH):
            config.read(CONFIGFILE_PATH)

            try:
                res = config.get('MONGODB', 'uri')
            except:
                res = DEFAULT_DB_URI

        else:
            config = configparser.ConfigParser()
            config.add_section('MONGODB')
            config.set('MONGODB', 'uri', DEFAULT_DB_URI)
            f = open('{}{}{}'.format(setting_path, os.sep, 'config.ini'), 'w')
            config.write(f)
            res = DEFAULT_DB_URI

        return res

    def get_config(
            self,
            section='MONGODB',
            option='uri',
            default_value=DEFAULT_DB_URI
    ):
        """[summary]

        Keyword Arguments:
            section {str} -- [description] (default: {'MONGODB'})
            option {str} -- [description] (default: {'uri'})
            default_value {[type]} -- [description] (default: {DEFAULT_DB_URI})

        Returns:
            [type] -- [description]
        """

        try:
            config = configparser.ConfigParser()
            config.read(CONFIGFILE_PATH)
            return config.get(section, option)
        except:
            res = self.client.quantaxis.usersetting.find_one(
                {'section': section})
            if res:
                return res.get(option, default_value)
            else:
                self.set_config(section, option, default_value)
                return default_value

    def set_config(
            self,
            section='MONGODB',
            option='uri',
            default_value=DEFAULT_DB_URI
    ):
        """[summary]

        Keyword Arguments:
            section {str} -- [description] (default: {'MONGODB'})
            option {str} -- [description] (default: {'uri'})
            default_value {[type]} -- [description] (default: {DEFAULT_DB_URI})

        Returns:
            [type] -- [description]
        """
        t = {'section': section, option: default_value}
        self.client.quantaxis.usersetting.update(
            {'section': section}, {'$set': t}, upsert=True)

        # if os.path.exists(CONFIGFILE_PATH):
        #     config.read(CONFIGFILE_PATH)
        #     self.lock.release()
        #     return self.get_or_set_section(
        #         config,
        #         section,
        #         option,
        #         default_value,
        #         'set'
        #     )

        #     # 排除某些IP
        #     # self.get_or_set_section(config, 'IPLIST', 'exclude', [{'ip': '1.1.1.1', 'port': 7709}])

        # else:
        #     f = open(CONFIGFILE_PATH, 'w')
        #     config.add_section(section)
        #     config.set(section, option, default_value)

        #     config.write(f)
        #     f.close()
        #     self.lock.release()
        #     return default_value

    def get_or_set_section(
            self,
            config,
            section,
            option,
            DEFAULT_VALUE,
            method='get'
    ):
        """[summary]

        Arguments:
            config {[type]} -- [description]
            section {[type]} -- [description]
            option {[type]} -- [description]
            DEFAULT_VALUE {[type]} -- [description]

        Keyword Arguments:
            method {str} -- [description] (default: {'get'})

        Returns:
            [type] -- [description]
        """

        try:
            if isinstance(DEFAULT_VALUE, str):
                val = DEFAULT_VALUE
            else:
                val = json.dumps(DEFAULT_VALUE)
            if method == 'get':
                return self.get_config(section, option)
            else:
                self.set_config(section, option, val)
                return val

        except:
            self.set_config(section, option, val)
            return val

    def env_config(self):
        return os.environ.get("MONGOURI", None)

    @property
    def client(self):
        return QA_util_sql_mongo_setting(self.mongo_uri)

    @property
    def client_async(self):
        return QA_util_sql_async_mongo_setting(self.mongo_uri)

    def change(self, ip, port):
        self.ip = ip
        self.port = port
        global DATABASE
        DATABASE = self.client
        return self


QASETTING = QA_Setting()
DATABASE = QASETTING.client.quantaxis
DATABASE_ASYNC = QASETTING.client_async.quantaxis