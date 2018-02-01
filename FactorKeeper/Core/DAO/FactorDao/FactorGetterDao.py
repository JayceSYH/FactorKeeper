"""
    This file defines getter functions relative to factor data.
    You should not modify this file.
"""


from Core.Conf.DatabaseConf import Schemas, Tables
from Core.DAO.TickDataDao import TickDataDao
from Core.DAO.FactorDao.FactorStatusDao import FactorStatusDao
from Core.Error.Error import Error
import pandas as pd
import traceback, datetime


class FactorGetterDao(object):
    def __init__(self, db_engine, logger):
        """
        :param db_engine: a sqlalchemy database engine
        :param logger:
        """
        self.db_engine = db_engine
        self.logger = logger.sub_logger(self.__class__.__name__)
        self.tick_dao = TickDataDao(db_engine, self.logger)
        self.status_dao = FactorStatusDao(db_engine, self.logger)

    # factor info ####################################################################

    def get_factor_list(self, con=None):
        """
        get created factor list
        :param con:
        :return: err_code, list of factor names
        """

        conn = con if con is not None else self.db_engine.connect()
        try:
            create_factor_sql = """
                        SELECT factor from "{0}"."{1}"
                    """.format(Schemas.SCHEMA_META, Tables.TABLE_FACTOR_LIST)

            factors = pd.read_sql(create_factor_sql, con=conn)['factor'].tolist()
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, []
        finally:
            if con is None:
                conn.close()

        return Error.SUCCESS, factors

    # version info ####################################################################

    def get_version_list(self, factor, con=None):
        """
        get exists versions of a factor
        :param factor:
        :param con:
        :return: err_code, list of version names
        """

        conn = con if con is not None else self.db_engine.connect()
        try:
            err, group_factor_name = self.get_group_factor(factor)
            if err:
                return err, None

            if group_factor_name is None:
                versions = pd.read_sql("""
                            SELECT version from "{0}"."{1}"
                            WHERE factor='{2}'
                        """.format(Schemas.SCHEMA_META, Tables.TABLE_FACTOR_VERSION, factor), con=conn)['version'].\
                    tolist()
            else:
                versions = pd.read_sql("""
                                            SELECT version from "{0}"."{1}"
                                            WHERE group_factor_name='{2}' AND sub_factor_name='{3}'
                                        """.format(Schemas.SCHEMA_META, Tables.TABLE_GROUP_FACTOR, group_factor_name,
                                                   factor), con=conn)[
                    'version'].tolist()
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            if con is None:
                conn.close()

        return Error.SUCCESS, versions

    def get_latest_version(self, factor, con=None):
        """
        Get the latest factor version of a factor
        :param con:
        :param factor:
        :return: err_code, latest version name
        """

        conn = con if con is not None else self.db_engine.connect()
        try:
            err, group_factor = self.get_group_factor(factor)
            if err:
                return err, None

            if group_factor is None:
                latest_version_df = pd.read_sql("""
                    SELECT version FROM "{0}"."{1}"
                    WHERE factor='{2}'
                    ORDER BY version_id DESC LIMIT 1
                """.format(Schemas.SCHEMA_META, Tables.TABLE_FACTOR_VERSION, factor), con=conn)

                if latest_version_df.shape[0] == 0:
                    return Error.ERROR_FACTOR_NOT_EXISTS, None
            else:
                latest_version_df = pd.read_sql("""
                                    SELECT version FROM "{0}"."{1}"
                                    WHERE sub_factor_name='{2}' AND group_factor_name='{3}'
                                    ORDER BY id DESC LIMIT 1
                                """.format(Schemas.SCHEMA_META, Tables.TABLE_GROUP_FACTOR, factor, group_factor)
                                                , con=conn)

                if latest_version_df.shape[0] == 0:
                    return Error.ERROR_FACTOR_NOT_EXISTS, None

            return Error.SUCCESS, latest_version_df['version'][0]

        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            if con is None:
                conn.close()

    def get_factor_version_id(self, factor, version, con=None):
        """
        Get factor version id
        :param con:
        :param factor:
        :param version:
        :return: err_code, factor_id
        """

        conn = con if con is not None else self.db_engine.connect()
        try:
            query_factor_version_id_df = pd.read_sql("""
                        SELECT version_id from "{0}"."{1}"
                        WHERE factor='{2}' AND version='{3}'
                    """.format(Schemas.SCHEMA_META, Tables.TABLE_FACTOR_VERSION,
                               factor, version), conn)

            if query_factor_version_id_df.shape[0] == 0:
                err, is_factor_exists = self.status_dao.is_factor_exists(factor, con=conn)
                if err:
                    return err, None

                if is_factor_exists:
                    return Error.ERROR_FACTOR_VERSION_NOT_EXISTS, None
                else:
                    return Error.ERROR_FACTOR_NOT_EXISTS, None
            else:
                return Error.SUCCESS, query_factor_version_id_df['version_id'].tolist()[0]
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            if con is None:
                conn.close()

    def get_factor_info_by_version_id(self, factor_version_id, con=None):
        """
        Get factor info by factor version id
        :param con:
        :param factor_version_id:
        :return: err_code, factor name, factor version
        """

        conn = con if con is not None else self.db_engine.connect()
        try:
            factor_version_df = pd.read_sql("""
                        SELECT factor, version FROM "{0}"."{1}"
                        WHERE version_id={2}
                    """.format(Schemas.SCHEMA_META, Tables.TABLE_FACTOR_VERSION, factor_version_id), conn)

            if factor_version_df.shape[0] == 0:
                return Error.ERROR_FACTOR_VERSION_NOT_EXISTS, None, None

            factor_id = factor_version_df['factor'].tolist()[0]
            factor_version = factor_version_df['version'].tolist()[0]

            return Error.SUCCESS, factor_id, factor_version
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None, None
        finally:
            if con is None:
                conn.close()

    # linkage info ####################################################################

    def get_linkage_id(self, factor, version, stock_code, con=None):
        """
        Get linkage id between a factor(factor group) version and a stock
        :param con:
        :param stock_code:
        :param factor: factor name or factor group name
        :param version:
        :return: err_code, linkage_id
        """

        conn = con if con is not None else self.db_engine.connect()
        try:
            err, version_id = self.get_factor_version_id(factor, version, con=conn)
            if err:
                return err, None

            link_id_df = pd.read_sql("""
                SELECT linkage_id FROM "{0}"."{1}"
                WHERE version_id={2} AND stock_code='{3}'
            """.format(Schemas.SCHEMA_META, Tables.TABLE_FACTOR_TICK_LINKAGE, version_id, stock_code),
                                     conn)

            if link_id_df.shape[0] == 0:
                return Error.ERROR_LINKAGE_NOT_EXISTS, None

            return Error.SUCCESS, link_id_df['linkage_id'].tolist()[0]
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            if con is None:
                conn.close()

    def get_linked_stock_list(self, factor, version, con=None):
        """
        List all linked stock names
        :param con:
        :param factor:
        :param version:
        :return: err_code, list of stock codes
        """

        conn = con if con is not None else self.db_engine.connect()
        try:
            err, version_id = self.get_factor_version_id(factor, version, con=conn)
            if err:
                return err, None

            linked_stocks = pd.read_sql("""
                        SELECT "stock_code" FROM "{0}"."{1}"
                        WHERE "version_id"='{2}'
                    """.format(Schemas.SCHEMA_META, Tables.TABLE_FACTOR_TICK_LINKAGE, version_id),
                                        con=conn)['stock_code'].tolist()

            return Error.SUCCESS, linked_stocks
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            if con is None:
                conn.close()

    # group factor ####################################################################

    def get_group_factor(self, sub_factor_name, default=None, con=None):
        """
        Get a sub factor's group factor name. If group factor not exists, return default value.
        :param sub_factor_name:
        :param default: if group factor not exists, return this value
        :param con:
        :return: err_code, group factor name
        """

        conn = con if con is not None else self.db_engine.connect()
        try:
            group_factor_df = pd.read_sql("""
                SELECT group_factor_name FROM "{0}"."{1}"
                WHERE sub_factor_name='{2}' LIMIT 1
            """.format(Schemas.SCHEMA_META, Tables.TABLE_GROUP_FACTOR, sub_factor_name), con=conn)

            if group_factor_df.shape[0] == 0:
                return Error.SUCCESS, default
            else:
                return Error.SUCCESS, group_factor_df['group_factor_name'][0]
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            if con is None:
                conn.close()

    def get_sub_factors(self, group_factor_name, version, con=None):
        """
        Get sub factor names.
        :param group_factor_name:
        :param version:
        :param con:
        :return: err_code, list of sub factor names
        """
        conn = con if con is not None else self.db_engine.connect()
        try:
            where_version_clause = " AND version='{}'".format(version) if version is not None else ""
            sub_factors = pd.read_sql("""
                SELECT DISTINCT sub_factor_name FROM "{0}"."{1}"
                WHERE group_factor_name='{2}' {3}
            """.format(Schemas.SCHEMA_META, Tables.TABLE_GROUP_FACTOR, group_factor_name, where_version_clause),
                                      con=conn)['sub_factor_name'].tolist()
            return Error.SUCCESS, sub_factors
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            if con is None:
                conn.close()

    # result info ##############################################################

    def load_factor_result(self, factor, version, stock, fetch_date, with_time=True, con=None):
        """
        Fetch factor data within a day
        :param stock:
        :param version:
        :param factor:
        :param con:
        :param with_time: time columns reserved in returned dataframe if "with_time" is True else False
        :param fetch_date: the day to fetch data
        :return: err_code, a dataframe contains factor data
        """

        conn = con if con is not None else self.db_engine.connect()
        try:
            err, group_factor = self.get_group_factor(factor, factor)
            if err:
                return err, None

            err, link_id = self.get_linkage_id(group_factor, version, stock)
            if err:
                return err, None

            where_clause = """
                                WHERE datetime >= '{0}' AND datetime < '{1}'
                            """. \
                format(fetch_date, fetch_date + datetime.timedelta(days=1))

            columns = """ "{}" """.format(factor) if not with_time else """ "{}", "datetime", "date" """.format(factor)

            ret = pd.read_sql("""
                SELECT {4} FROM "{0}"."{1}{2}" {3} ORDER BY datetime
            """.format(Schemas.SCHEMA_FACTOR_DATA, Tables.TABLE_FACTOR_RESULT_PREFIX, link_id, where_clause, columns),
                              con=conn)

            if ret.shape[0] == 0:
                return Error.ERROR_FACTOR_RESULT_NOT_EXISTS, None

            return Error.SUCCESS, ret
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            if con is None:
                conn.close()

    def load_factor_result_by_range(self, factor, version, stock, start_date, end_date, with_time=True, con=None):
        """
        load factor data by a time range
        :param factor:
        :param version:
        :param stock:
        :param start_date:
        :param end_date:
        :param with_time:
        :param con:
        :return: err_code, a dataframe contains factor data
        """

        conn = con if con is not None else self.db_engine.connect()
        try:
            err, group_factor = self.get_group_factor(factor, factor)
            if err:
                return err, None

            err, link_id = self.get_linkage_id(group_factor, version, stock)
            if err:
                return err, None

            where_clause = """
                                            WHERE datetime >= '{0}' AND datetime < '{1}'
                                        """. \
                format(start_date, end_date + datetime.timedelta(days=1))

            columns = """ "{}" """.format(factor) if not with_time else """ "{}", "datetime", "date" """.format(factor)

            ret = pd.read_sql("""
                            SELECT {4} FROM "{0}"."{1}{2}" {3} ORDER BY datetime
                        """.format(Schemas.SCHEMA_FACTOR_DATA, Tables.TABLE_FACTOR_RESULT_PREFIX, link_id, where_clause,
                                   columns),
                              con=conn)

            if ret.shape[0] == 0:
                return Error.ERROR_FACTOR_RESULT_NOT_EXISTS, None

            return Error.SUCCESS, ret
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            if con is None:
                conn.close()

    # updated dates info
    def get_updated_dates_list(self, factor, version, stock, con=None):
        conn = con if con is not None else self.db_engine.connect()
        try:
            err, link_id = self.get_linkage_id(factor, version, stock, con=conn)
            if err:
                return err, None

            updated_dates = pd.read_sql("""
                SELECT factor_date FROM "{0}"."{1}"
                WHERE linkage_id='{2}' AND (end_update_time IS NOT NULL) ORDER BY factor_date
            """.format(Schemas.SCHEMA_META, Tables.TABLE_FACTOR_UPDATE_LOG, link_id), con=conn)['factor_date'].tolist()

            return Error.SUCCESS, updated_dates
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            if con is None:
                conn.close()
