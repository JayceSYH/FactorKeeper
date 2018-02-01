"""
    This file define functions querying factor status.
    You should not modify this file.
"""


from Core.Conf.DatabaseConf import Schemas, Tables
from Core.Error.Error import Error
from Core.DAO.TickDataDao import TickDataDao
import pandas as pd
import traceback


class FactorStatusDao(object):
    def __init__(self, db_engine, logger):
        """
        :param db_engine: a sqlalchemy database engine
        :param logger:
        """
        self.db_engine = db_engine
        self.logger = logger.sub_logger(self.__class__.__name__)
        self.tick_dao = TickDataDao(db_engine, self.logger)

    def __is_factor_exists_as_sub_factor(self, factor, conn):
        """
        Check weather factor exists as a sub factor
        :param factor:
        :return: err_code, True if factor is a sub factor else False
        """

        try:
            factor_count = pd.read_sql("""
                SELECT count(1) as factor_count FROM "{0}"."{1}"
                WHERE sub_factor_name='{2}'
            """.format(Schemas.SCHEMA_META, Tables.TABLE_GROUP_FACTOR, factor), con=conn)['factor_count'][0]
            return Error.SUCCESS, factor_count > 0
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None

    def is_factor_exists(self, factor, check_group_factor=True, con=None):
        """
        Check weather factor exists
        :param con:
        :param check_group_factor: Check weather factor is a group factor if "check_group_factor" is True
        :param factor:
        :return: err_code, True if factor exists, else False
        """

        conn = con if con is not None else self.db_engine.connect()
        try:
            factor_count = pd.read_sql("""
                            SELECT COUNT(1) AS factor_count FROM "{0}"."{1}"
                            WHERE factor='{2}'
                        """.format(Schemas.SCHEMA_META, Tables.TABLE_FACTOR_LIST, factor),
                                       conn)['factor_count'].tolist()[0]

            if factor_count == 0 and check_group_factor:
                return self.__is_factor_exists_as_sub_factor(factor, conn)
            else:
                return Error.SUCCESS, factor_count > 0
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            if con is None:
                conn.close()

    def is_factor_version_exists(self, factor, version, check_group_factor=True, con=None):
        """
        Check weather factor version exists
        :param factor:
        :param version:
        :param check_group_factor: Check weather factor is a group factor if "check_group_factor" is True
        :param con:
        :return: err_code, True if exists, else False
        """

        conn = con if con is not None else self.db_engine.connect()
        try:
            count = pd.read_sql("""
                SELECT count(1) as version_count FROM "{0}"."{1}"
                WHERE factor='{2}' AND version='{3}'
            """.format(Schemas.SCHEMA_META, Tables.TABLE_FACTOR_VERSION, factor, version), conn)[
                'version_count'].tolist()[0]

            return Error.SUCCESS, count > 0
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            if con is None:
                conn.close()

    def is_factor_table_exists(self, link_id, con=None):
        """
        Check weather factor table exists
        :param link_id:
        :param con:
        :return: err_code, True if exists, else False
        """
        conn = con if con is not None else self.db_engine.connect()
        try:
            count = pd.read_sql("""
                        SELECT count(1) as table_count FROM pg_tables
                        WHERE schemaname='{0}' AND tablename='{1}{2}'
                    """.format(Schemas.SCHEMA_FACTOR_DATA, Tables.TABLE_FACTOR_RESULT_PREFIX, link_id,),
                                conn)['table_count'].tolist()[0]

            return Error.SUCCESS, count > 0
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            if con is None:
                conn.close()
