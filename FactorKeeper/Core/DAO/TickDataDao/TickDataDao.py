"""
    This file defines tick data dao interfaces which may be used by other modules.
"""


from Core.DAO.TableMakerDao import TableMaker
from Core.Error.Error import Error
from Core.Conf.TickDataConf import TickDataConf
from Core.DAO.TickDataDao.TickDataImportDao import TickDataImportDao
from Core.DAO.TickDataDao.FactorKeeperDBTickDataDao import FactorKeeperDBTickDataDao
from Core.DAO.TickDataDao.StockViewTickDataDao import StockViewTickDataDao


class TickDataDao(object):
    def __init__(self, db_engine, logger):
        """
        :param db_engine: a sqlalchemy database engine
        :param logger:
        """
        self.db_engine = db_engine
        self.logger = logger.sub_logger(self.__class__.__name__)
        self.table_maker = TableMaker(db_engine, self.logger)
        self.tick_data_source_dao = TickDataImportDao(db_engine, self.logger)
        self.factor_keeper_dao = FactorKeeperDBTickDataDao(db_engine, self.logger)
        self.stock_view_dao = StockViewTickDataDao(db_engine, self.logger)

    # common interface
    def is_stock_available(self, stock_code):
        if TickDataConf.is_stock_view(stock_code):
            return self.stock_view_dao.is_stock_view_exists(stock_code)
        else:
            return self.tick_data_source_dao.is_stock_available(stock_code)

    def list_available_tick_dates(self, stock_code):
        if TickDataConf.is_stock_view(stock_code):
            return self.stock_view_dao.get_stock_view_available_dates(stock_code)
        else:
            return self.tick_data_source_dao.get_tick_dates_from_data_source(stock_code)

    def list_updated_dates(self, stock_code):
        return self.factor_keeper_dao.list_tick_dates(stock_code)

    def load_updated_tick_data(self, stock_code, day, columns=None):
        if TickDataConf.is_stock_view(stock_code):
            return self.stock_view_dao.load_stock_view_data(stock_code, day)
        else:
            return self.factor_keeper_dao.load_data_by_code(stock_code, day, columns=columns)

    def is_tick_data_newest_version(self, stock_code):
        err, available_dates = self.list_available_tick_dates(stock_code)

        if err:
            return err, None

        err, new_db_dates = self.list_updated_dates(stock_code)
        if err:
            return err, None

        return Error.SUCCESS, len(set(available_dates) - set(new_db_dates)) == 0

    # outer source interface
    def load_data_from_outer_source(self, stock_code, day):
        return self.tick_data_source_dao.get_tick_data_from_data_source_on_day(stock_code, day)

    # factor keeper db interface
    def is_stock_table_exists_in_factor_keeper_db(self, stock_code):
        return self.factor_keeper_dao.is_stock_table_exists(stock_code)

    def is_stock_data_updated_in_factor_keeper_db(self, stock_code):
        return self.factor_keeper_dao.is_stock_data_exists(stock_code)

    def create_new_update_log(self, stock_code, day):
        return self.factor_keeper_dao.new_stock_tick_data_log(stock_code, day)

    def finish_update_log(self, log_id):
        return self.factor_keeper_dao.finish_stock_tick_data_log(log_id)

    # stock view interface(inherit from factor keeper tick db)
    def create_stock_view(self, stock_code, stock_relation):
        return self.stock_view_dao.create_stock_view(stock_code, stock_relation)

    def get_stock_view_relation(self, stock_code):
        return self.stock_view_dao.get_stock_view_relation(stock_code)

    def create_stock_view_index_if_not_exists(self, stock_code):
        return self.stock_view_dao.create_stock_view_index_if_not_exists(stock_code)

    def is_stock_view_table_exists(self, stock_code):
        return self.stock_view_dao.is_stock_view_table_exists(stock_code)
