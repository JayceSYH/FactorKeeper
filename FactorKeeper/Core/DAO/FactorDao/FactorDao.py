"""
    This file defines interfaces may be used by other modules.
    You should not modify this file.
"""


from Core.DAO.FactorDao.CreateFactorMetaDao import CreateFactorMetaDao
from Core.DAO.FactorDao.FactorGetterDao import FactorGetterDao
from Core.DAO.FactorDao.FactorStatusDao import FactorStatusDao
from Core.DAO.FactorDao.FactorAssistDao import FactorAssistDao
from Core.Error.Error import Error


class FactorDao(object):
    def __init__(self, db_engine, logger):
        """
        :param db_engine: a sqlalchemy database engine
        :param logger:
        """
        self.db_engine = db_engine
        self.logger = logger.sub_logger(self.__class__.__name__)
        self.creator_dao = CreateFactorMetaDao(db_engine, self.logger)
        self.getter_dao = FactorGetterDao(db_engine, self.logger)
        self.status_dao = FactorStatusDao(db_engine, self.logger)
        self.assist_dao = FactorAssistDao(db_engine, self.logger)

    def create_factor(self, factor):
        return self.creator_dao.create_factor(factor)

    def create_group_factor(self, factors):
        return self.creator_dao.create_group_factor(factors)

    def create_factor_version(self, factor, version, code_file):
        return self.creator_dao.create_factor_version(factor, version, code_file)

    def create_group_factor_version(self, group_factor_name, factors, version, code_file):
        return self.creator_dao.create_group_factor_version(group_factor_name, factors, version, code_file)

    def create_linkage(self, factor, version, stock):
        err, factor = self.get_group_factor(factor, default=factor)
        if err:
            return err
        return self.creator_dao.create_linkage(factor, version, stock)

    def load_factor_result(self, factor, version, stock, fetch_date, with_time=True):
        return self.getter_dao.load_factor_result(factor, version, stock, fetch_date, with_time=with_time)

    def load_factor_result_by_range(self, factor, version, stock, start_date, end_date, with_time=True):
        return self.getter_dao.load_factor_result_by_range(factor, version, stock, start_date, end_date,
                                                           with_time=with_time)

    def list_factors(self):
        return self.getter_dao.get_factor_list()

    def list_versions(self, factor):
        return self.getter_dao.get_version_list(factor)

    def list_linked_stocks(self, factor, version):
        err, factor = self.get_group_factor(factor, default=factor)
        if err:
            return err
        return self.getter_dao.get_linked_stock_list(factor, version)

    def list_updated_dates(self, factor, version, stock):
        err, factor = self.get_group_factor(factor, default=factor)
        if err:
            return err, None
        return self.getter_dao.get_updated_dates_list(factor, version, stock)

    def get_group_factor(self, sub_factor_name, default=None):
        return self.getter_dao.get_group_factor(sub_factor_name, default=default)

    def get_sub_factors(self, group_factor_name, version):
        return self.getter_dao.get_sub_factors(group_factor_name, version)

    def get_latest_version(self, factor):
        return self.getter_dao.get_latest_version(factor)

    def get_linkage_id(self, factor, version, stock):
        err, factor = self.get_group_factor(factor, default=factor)
        if err:
            return err
        return self.getter_dao.get_linkage_id(factor, version, stock)

    def is_factor_table_exists(self, link_id):
        return self.status_dao.is_factor_table_exists(link_id)

    def is_group_factor(self, group_factor_name):
        err, sub_factors = self.getter_dao.get_sub_factors(group_factor_name, version=None)
        if err:
            return err, None
        return Error.SUCCESS, len(sub_factors) > 0

    def clean_old_factor_data(self, factor, version, stock, day):
        err, factor = self.get_group_factor(factor, default=factor)
        if err:
            return err
        return self.assist_dao.clean_old_factor_data(factor, version, stock, day)

    def start_update_log(self, link_id, day):
        return self.assist_dao.start_update_log(link_id, day)

    def finish_update_log(self, log_id):
        return self.assist_dao.finish_update_log(log_id)

    def get_factor_version_code(self, factor, version):
        err, factor = self.get_group_factor(factor, default=factor)
        if err:
            return err
        return self.assist_dao.get_factor_version_code(factor, version)
