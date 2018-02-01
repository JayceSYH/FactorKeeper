"""
    This file defines the dao of tick data saved in factor keeper database.
    You should not modify this file.
"""


from Core.DAO.TableMakerDao import TableMaker
from Core.Conf.DatabaseConf import Schemas, Tables
from Core.Error.Error import Error
import traceback, datetime
import pandas as pd


class FactorKeeperDBTickDataDao(object):
    """
        This dao defines functions relative to tick data save in factor keeper database.
    """
    def __init__(self, db_engine, logger):
        """
        :param db_engine: sqlalchemy database engine
        :param logger:
        """
        self.db_engine = db_engine
        self.logger = logger.sub_logger(self.__class__.__name__)
        self.table_maker = TableMaker(db_engine, self.logger)

    def load_data_by_code(self, stock_code, fetch_date, columns=None):
        """
        Load tick data from factor keeper database.
        :param stock_code: stock code
        :param fetch_date: the date to fetch
        :param columns: list of columns to fetch, must not be None or empty list
        :return: err_code, dataframe of tick data
        """

        conn = self.db_engine.connect()
        try:
            if isinstance(fetch_date, datetime.datetime):
                fetch_date = fetch_date.date()
            where_clause = """
                WHERE "date"='{}'
            """. \
                format(fetch_date)

            column_str = "*" if columns is None else ", ".join(columns)

            load_data_sql = """
                    SELECT {4} FROM "{0}"."{1}{2}" {3} ORDER BY datetime; 
                """.format(Schemas.SCHEMA_TICK_DATA, Tables.TABLE_TICK_STOCK_PREFIX, stock_code, where_clause,
                           column_str)

            ret_df = pd.read_sql(load_data_sql, conn)
            if ret_df.shape[0] == 0:
                self.logger.log_error("tick data not exists(stock code:{0} day:{1})".format(stock_code, fetch_date))
                return Error.ERROR_TICK_DATA_NOT_EXISTS, None
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            conn.close()

        return Error.SUCCESS, ret_df

    def list_tick_dates(self, stock_code):
        """
        list dates where stock tick data exists
        :param stock_code: stock code
        :return: err_code, list of date object
        """

        conn = self.db_engine.connect()
        try:
            get_tick_dates_sql = """
                        SELECT DISTINCT "update_date" FROM "{0}"."{1}"
                        WHERE end_update_time IS NOT NULL AND stock_code='{2}'
                    """.format(Schemas.SCHEMA_META, Tables.TABLE_TICK_UPDATE_LOGS, stock_code)

            res = pd.read_sql(get_tick_dates_sql, conn)['update_date'].tolist()
            res.sort()
            return Error.SUCCESS, res
        except Exception:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            conn.close()

    def is_stock_table_exists(self, stock_code):
        """
        Check is stock tick data table exists in factor keeper database
        :param stock_code: stock code
        :return: err_code, True if exists, else False
        """
        conn = self.db_engine.connect()
        try:
            num = pd.read_sql("""
                SELECT count(1) as num FROM pg_tables
                WHERE schemaname='{0}' and tablename='{1}'
            """.format(Schemas.SCHEMA_TICK_DATA, Tables.TABLE_TICK_STOCK_PREFIX + stock_code), con=conn)['num'][0]

            if num > 0:
                return Error.SUCCESS, True
            else:
                return Error.SUCCESS, False

        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            conn.close()

    def is_stock_data_exists(self, stock_code):
        """
        Check is stock tick data exists in factor keeper database
        :param stock_code: stock code
        :return: err_code, True if exists, else False
        """
        conn = self.db_engine.connect()
        try:
            num = pd.read_sql("""
                SELECT count(1) AS num FROM "{0}"."{1}"
                WHERE stock_code='{2}' AND end_update_time IS NOT NULL
            """.format(Schemas.SCHEMA_META, Tables.TABLE_TICK_UPDATE_LOGS,
                       stock_code), con=conn)['num'][0]
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            conn.close()

        return Error.SUCCESS, num > 0

    def new_stock_tick_data_log(self, stock_code, update_date):
        """
        create a start update log
        :param stock_code: stock code
        :param update_date: date to be update
        :return: err_code, log id
        """
        conn = self.db_engine.connect()
        try:
            now = datetime.datetime.now()

            add_stock_list_log_sql = """
                                INSERT INTO "{0}"."{1}"
                                (
                                start_update_time,
                                stock_code,
                                update_date
                                )
                                VALUES('{2}', '{3}', '{4}');
                            """.format(Schemas.SCHEMA_META, Tables.TABLE_TICK_UPDATE_LOGS,
                                       now, stock_code, update_date)

            conn.execute(add_stock_list_log_sql)

            # fetch this row and return log id
            fetch_row_sql = """
                        SELECT log_id FROM "{0}"."{1}" 
                        WHERE start_update_time='{2}' AND stock_code='{3}' AND update_date='{4}'
                    """.format(Schemas.SCHEMA_META, Tables.TABLE_TICK_UPDATE_LOGS,
                               now, stock_code, update_date)

            id_df = pd.read_sql(fetch_row_sql, conn)
            if id_df.shape[0] == 0:
                self.logger.log_error("Unable to fetch log id")
                return Error.ERROR_SERVER_INTERNAL_ERROR, None
            else:
                return Error.SUCCESS, id_df['log_id'][0]
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            conn.close()

    def finish_stock_tick_data_log(self, log_id):
        """
        create a finish update log
        :param log_id: log id return by "new_stock_tick_data_log"
        :return: err_code
        """
        finish_stock_list_log_sql = """
                    UPDATE "{0}"."{1}" 
                    SET end_update_time = '{2}'
                    WHERE log_id = {3};
                """.format(Schemas.SCHEMA_META, Tables.TABLE_TICK_UPDATE_LOGS,
                           datetime.datetime.now(), log_id)

        conn = self.db_engine.connect()
        try:
            conn.execute(finish_stock_list_log_sql)
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            conn.close()

        return Error.SUCCESS
