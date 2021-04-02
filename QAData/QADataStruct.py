from functools import lru_cache

from QAData.base_datastruct import _quotation_base
from QAData.data_resample import QA_data_cryptocurrency_min_resample
from QAUtil.QASetting import DATABASE


class QA_DataStruct_CryptoCurrency_min(_quotation_base):
    """
    struct for crypto asset_
    """

    def __init__(self, DataFrame, dtype='crypto_currency_min', if_fq=False):
        super().__init__(DataFrame, dtype, if_fq)
        self.type = dtype
        self.data = self.data.sort_index()
        self.if_fq = if_fq

    # 抽象类继承
    def choose_db(self):
        self.mongo_coll = DATABASE.cryptocurrency_min

    @property
    @lru_cache()
    def min5(self):
        return self.resample('5min')

    @property
    @lru_cache()
    def min15(self):
        return self.resample('15min')

    @property
    @lru_cache()
    def min30(self):
        return self.resample('30min')

    @property
    @lru_cache()
    def min60(self):
        return self.resample('60min')

    def __repr__(self):
        return '< QA_DataStruct_CryptoCurrency_min with {} securities >'.format(
            len(self.code)
        )

    __str__ = __repr__

    def resample(self, level):
        try:
            return self.add_funcx(QA_data_cryptocurrency_min_resample,
                                  level).sort_index()
        except Exception as e:
            print('QA ERROR : FAIL TO RESAMPLE {}'.format(e))
            return None