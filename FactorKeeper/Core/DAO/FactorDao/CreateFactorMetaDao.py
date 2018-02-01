"""
    This file defines functions used to create factor meta info:
        -create a new factor
        -create a group factor
        -create a new factor version
        -create a group factor version
        -create a linkage between factor version(or group factor version) and stock tick data(or stock view data)

    You should not modify this file
"""


from Core.Conf.DatabaseConf import Schemas, Tables
from Core.Conf.FactorConf import FactorConf
from Core.Error.Error import Error
from Core.DAO.FactorDao.FactorGetterDao import FactorGetterDao
from Core.DAO.FactorDao.FactorStatusDao import FactorStatusDao
from Core.DAO.TickDataDao.TickDataDao import TickDataDao
import traceback, datetime


class CreateFactorMetaDao(object):
    def __init__(self, db_engine, logger):
        """
        :param db_engine: a sqlalchemy database engine
        :param logger:
        """
        self.db_engine = db_engine
        self.logger = logger.sub_logger(self.__class__.__name__)
        self.getter_dao = FactorGetterDao(db_engine, self.logger)
        self.status_dao = FactorStatusDao(db_engine, self.logger)
        self.tick_dao = TickDataDao(db_engine, self.logger)

    def __create_factor(self, factor, conn):
        try:
            conn.execute("""
                INSERT INTO "{0}"."{1}"(factor, create_time)
                VALUES (%s, %s)
            """.format(Schemas.SCHEMA_META, Tables.TABLE_FACTOR_LIST), (factor, datetime.datetime.now()))
            return Error.SUCCESS
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED

    def __create_factor_version(self, factor, factor_version, code, conn):
        """
        create factor version
        :param factor:
        :param factor_version:
        :param code: code file defined by user which contains functions used to calculate factor given tick data of a day
         in python language.
        :return: err_code
        """

        try:
            conn.execute("""
                        INSERT INTO "{0}"."{1}"(factor, version, code)
                        VALUES ('{2}', '{3}', %s)
                    """.format(Schemas.SCHEMA_META, Tables.TABLE_FACTOR_VERSION,
                               factor, factor_version), (code,))
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED

        return Error.SUCCESS

    def __link_group_factor(self, group_factor, sub_factor, version, conn):
        """
        link group factor with its sub factor
        :param group_factor:
        :param sub_factor:
        :param version: group factor version
        :param conn: db connection
        :return:
        """
        try:
            conn.execute("""
                INSERT INTO "{0}"."{1}"(group_factor_name, sub_factor_name, version)
                VALUES (%s, %s, %s)
            """.format(Schemas.SCHEMA_META, Tables.TABLE_GROUP_FACTOR), (group_factor, sub_factor, version))
            return Error.SUCCESS
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED

    def create_factor(self, factor, con=None):
        """
        create a factor
        :param con: create a new db connection if con is none
        :param factor:
        :return: err_code
        """

        conn = con if con is not None else self.db_engine.connect()
        try:
            err, is_exists = self.status_dao.is_factor_exists(factor, con=conn)
            if err:
                return err

            if is_exists:
                return Error.ERROR_FACTOR_ALREADY_EXISTS
            else:
                return self.__create_factor(factor, conn)
        except Exception:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            if con is None:
                conn.close()

    def create_group_factor(self, factors, con=None):
        """
        create a group factor
        :param factors:
        :param con:
        :return: err_code
        """
        group_factor_name = FactorConf.get_group_factor_name(factors)

        conn = con if con is not None else self.db_engine.connect()
        try:
            with conn.begin():
                # check if factor exists
                for factor in factors + [group_factor_name]:
                    err, is_factor_exists = self.status_dao.is_factor_exists(factor, con=conn)
                    if err:
                        return err, None
                    elif is_factor_exists:
                        return Error.ERROR_FACTOR_ALREADY_EXISTS, "factor '{}' already exists".format(factor)

                # create factor in factor list table
                err = self.__create_factor(group_factor_name, conn)
                if err:
                    return err, None

                return Error.SUCCESS, group_factor_name
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            if con is None:
                conn.close()

    def create_factor_version(self, factor, factor_version, code, con=None):
        """
        create a new factor version
        :param con:
        :param factor:
        :param factor_version:
        :param code: code file defined by user which contains functions used to calculate factor given tick data of a day
         in python language.
        :return: err_code
        """

        conn = con if con is not None else self.db_engine.connect()
        try:
            # if factor exists
            err, is_factor_exists = self.status_dao.is_factor_exists(factor, con=conn)
            if err:
                return err

            if not is_factor_exists:
                return Error.ERROR_FACTOR_NOT_EXISTS

            # if version has already exists
            err, is_version_exists = self.status_dao.is_factor_version_exists(factor, factor_version, con=conn)
            if err:
                return err

            if is_version_exists:
                return Error.ERROR_FACTOR_VERSION_ALREADY_EXISTS

            # create factor version in database
            return self.__create_factor_version(factor, factor_version, code, conn)
        finally:
            if con is None:
                conn.close()

    def create_group_factor_version(self, group_factor_name, factors, version, code_file, con=None):
        """
        create a group factor version
        :param group_factor_name: group factor name
        :param factors: group factor's sub factors
        :param version: group factor version
        :param code_file: code file defined by user which contains functions used to calculate factor given tick data of a day
         in python language.
        :param con:
        :return:
        """

        if len(factors) == 0:
            return Error.ERROR_PARAMETER_MISSING_OR_INVALID

        conn = con if con is not None else self.db_engine.connect()
        try:
            # is version exists
            err, is_version_exists = self.status_dao.is_factor_version_exists(group_factor_name, version)
            if err:
                return err

            if is_version_exists:
                return Error.ERROR_FACTOR_VERSION_ALREADY_EXISTS

            # check sub factor status
            err, sub_factors = self.getter_dao.get_sub_factors(group_factor_name, version=None, con=conn)
            if err:
                return err

            sub_factors = set(sub_factors)
            for factor in factors:
                if factor not in sub_factors:
                    err, is_factor_exists = self.status_dao.is_factor_exists(factor, con=conn)
                    if err:
                        return err

                    if is_factor_exists:
                        return Error.ERROR_SUB_FACTOR_CONFLICT_WITH_OTHER_FACTOR

            with conn.begin():
                # create group factor-sub factor linkage
                for factor in factors:
                    err = self.__link_group_factor(group_factor_name, factor, version, conn)
                    if err:
                        self.logger.log_error("failed to create group factor linkage with sub factor(code:{})".
                                              format(err))
                        return err

            # create version
            return self.__create_factor_version(group_factor_name, version, code_file, conn)
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            if con is None:
                conn.close()

    def create_linkage(self, factor, version, stock, con=None):
        """
        :param factor:
        :param version:
        :param stock:
        :param con:
        :return:
        """

        conn = con if con is not None else self.db_engine.connect()
        try:
            now = datetime.datetime.now()

            # check is stock exists
            err, is_stock_exists = self.tick_dao.is_stock_available(stock)
            if err:
                return err

            if not is_stock_exists:
                return Error.ERROR_TICK_STOCK_NOT_EXISTS

            # check if linkage exists
            err, link_id = self.getter_dao.get_linkage_id(factor, version, stock, con=conn)
            if err and err != Error.ERROR_LINKAGE_NOT_EXISTS:
                return err
            elif not err:
                return Error.ERROR_LINKAGE_ALREADY_EXISTS

            err, version_id = self.getter_dao.get_factor_version_id(factor, version, con=conn)
            if err:
                return err

            conn.execute("""
                INSERT INTO "{0}"."{1}"(version_id, stock_code, create_time, update_time)
                VALUES(%s, %s, %s, %s)
            """.format(Schemas.SCHEMA_META, Tables.TABLE_FACTOR_TICK_LINKAGE), (version_id, stock, now, now))
            return Error.SUCCESS
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            if con is None:
                conn.close()
