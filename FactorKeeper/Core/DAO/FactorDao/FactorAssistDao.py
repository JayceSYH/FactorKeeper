"""
    Some Assist functions used in factor keeper.
    You should not modify this file.
"""


from Core.Conf.DatabaseConf import Schemas, Tables
from Core.Error.Error import Error
from Core.DAO.FactorDao.FactorGetterDao import FactorGetterDao
from Core.DAO.FactorDao.FactorStatusDao import FactorStatusDao
import traceback, datetime
import pandas as pd


class FactorAssistDao(object):
    def __init__(self, db_engine, logger):
        """
        :param db_engine: a sqlalchemy database engine
        :param logger:
        """
        self.db_engine = db_engine
        self.logger = logger.sub_logger(self.__class__.__name__)
        self.getter_dao = FactorGetterDao(db_engine, self.logger)
        self.status_dao = FactorStatusDao(db_engine, self.logger)

    def clean_old_factor_data(self, factor, version, stock_code, day, con=None):
        """
        Clean old factor data in factor keeper database. It's always called before inserting
        factor data into factor keeper database in case of old factor data reserved in database
        during an unfinished(failed) updating progress.
        :param factor:
        :param version:
        :param stock_code:
        :param day:
        :param con:
        :return: err_code
        """

        conn = con if con is not None else self.db_engine.connect()
        try:
            err, link_id = self.getter_dao.get_linkage_id(factor, version, stock_code, con=conn)
            if err:
                return err

            err, is_table_exists = self.status_dao.is_factor_table_exists(link_id, con=conn)
            if err:
                return err

            if not is_table_exists:
                return Error.SUCCESS

            # 创建条件子句
            where_clause = """
                                    WHERE datetime >= '{0}' AND datetime < '{1}'
                                """. \
                format(day, day + datetime.timedelta(days=1))

            conn.execute("""
                DELETE FROM "{0}"."{1}" {2}
            """.format(Schemas.SCHEMA_FACTOR_DATA, Tables.TABLE_FACTOR_RESULT_PREFIX + str(link_id), where_clause))
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            if con is None:
                conn.close()

        return Error.SUCCESS

    def start_update_log(self, linkage_id, date, con=None):
        """
        create a start update log
        :param linkage_id:
        :param date:
        :param con:
        :return: err_code, log id
        """

        conn = con if con is not None else self.db_engine.connect()
        try:
            now = datetime.datetime.now()

            conn.execute("""
                INSERT INTO "{0}"."{1}"(linkage_id, factor_date, start_update_time)
                VALUES({2}, '{3}', '{4}')
            """.format(Schemas.SCHEMA_META, Tables.TABLE_FACTOR_UPDATE_LOG,
                       linkage_id, date, now))

            log_id = pd.read_sql("""
                SELECT log_id FROM "{0}"."{1}" 
                WHERE linkage_id={2} AND factor_date='{3}' AND start_update_time='{4}'
            """.format(Schemas.SCHEMA_META, Tables.TABLE_FACTOR_UPDATE_LOG,
                       linkage_id, date, now), con=conn)['log_id'].tolist()[0]

            return Error.SUCCESS, log_id
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            if con is None:
                conn.close()

    def finish_update_log(self, log_id, con=None):
        """
        Create a finish update log
        :param log_id: log id returned by start_update_log
        :param con:
        :return: err_code
        """

        conn = con if con is not None else self.db_engine.connect()
        try:
            conn.execute("""
                UPDATE "{0}"."{1}"
                SET end_update_time='{2}' WHERE log_id={3}
            """.format(Schemas.SCHEMA_META, Tables.TABLE_FACTOR_UPDATE_LOG,
                       datetime.datetime.now(), log_id))
            return Error.SUCCESS

        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            if con is None:
                conn.close()

    def get_factor_version_code(self, factor, version, con=None):
        """
        Get code file linked with an factor(factor group) version
        :param factor: factor/factor group name
        :param version:
        :param con:
        :return:
        """
        conn = con if con is not None else self.db_engine.connect()
        try:
            code = pd.read_sql("""
                SELECT code FROM "{0}"."{1}"
                WHERE factor='{2}' AND version='{3}'
            """.format(Schemas.SCHEMA_META, Tables.TABLE_FACTOR_VERSION,
                       factor, version), con=conn)

            if code.shape[0] == 0:
                self.logger.log_error("factor version not found({0}:{1})".format(factor, version))
                return Error.ERROR_FACTOR_VERSION_NOT_EXISTS, None
            else:
                return Error.SUCCESS, code['code'][0]
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            if con is None:
                conn.close()
