"""
    Import tick data from outer source(Can be an exists database or other data source).
    You can modify the following functions to make sure the data importer to work properly.
"""


from Core.Error.Error import Error
from Core.Conf.DatabaseConf import TickDataSourceDatabaseConf
import traceback, datetime
import pandas as pd


class TickDataImportDao(object):
    """
        This class lists the interfaces used to import tick data from outer source.
        Default implementation import data from an existing database, you may modify
        its functions to adapt to your data source.
    """
    def __init__(self, db_engine, logger):
        """
        :param db_engine: a sql alchemy database engine
        :param logger:
        """
        self.db_engine = db_engine
        self.logger = logger.sub_logger(self.__class__.__name__)
        self.db_importer = OuterDBDataImportDao(db_engine, logger)

    def get_tick_dates_from_data_source(self, stock_code):
        """
        Import stock tick data by stock code. Maker sure the returned dataframe has
        columns including "date" and "datetime"
        :param stock_code: stock code
        :return: err_code, tick data dataframe with "date" and "datetime" column
        """

        return self.db_importer.get_tick_dates_from_old_db(stock_code)

    def is_stock_available(self, stock_code):
        """
        Check data source weather stock tick data is available
        :param stock_code: stock code
        :return: err_code, True if available else False
        """

        return self.db_importer.is_stock_exists_in_old_db(stock_code)

    def get_tick_data_from_data_source_on_day(self, stock_code, day):
        """
        Fetch stock data on date specified by "day" parameter from data source and return it
        as a dataframe
        :param stock_code: stock code
        :param day: the day of data you want to fetch
        :return: err_code, a dataframe with tick data on the day specified by parameter "day"
        """

        return self.db_importer.get_tick_data_from_old_db_on_day(stock_code, day)


class OuterDBDataImportDao(object):
    """
        This class is an implementation of interfaces defined by TickDataImportDao which
        fetch data from an existing database.
    """
    def __init__(self, db_engine, logger):
        """
        :param db_engine: a sql alchemy database engine
        :param logger:
        """
        self.db_engine = TickDataSourceDatabaseConf.create_db_engine()
        self.logger = logger.sub_logger(self.__class__.__name__)

    def get_tick_dates_from_old_db(self, stock_code):
        """
        Implementation of "get_tick_dates_from_data_source", make sure your
        source database table has column "date"
        :param stock_code: stock code
        :return: err_code, tick data dataframe
        """

        get_tick_dates_sql = """
                            SELECT DISTINCT "date" FROM "{0}"."{1}"
                            WHERE {2}='{3}'
                        """.format(TickDataSourceDatabaseConf.SCHEMA, TickDataSourceDatabaseConf.TABLE,
                                   TickDataSourceDatabaseConf.STOCK_CODE_COL_NAME, stock_code)

        conn = self.db_engine.connect()
        try:
            res = pd.read_sql(get_tick_dates_sql, conn)['date'].tolist()
            res = [datetime.datetime.strptime(date, "%Y-%m-%d").date() for date in res]
            res.sort()
            return Error.SUCCESS, res
        except Exception:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, []
        finally:
            conn.close()

    def is_stock_exists_in_old_db(self, stock_code):
        """
        Implementation of "is_stock_available".
        :param stock_code: stock code
        :return: err_code, True if stock data exists in data source database, else False
        """

        sql = """
            SELECT COUNT(1) as stock_count FROM "{0}"."{1}"
            WHERE {2}='{3}'
        """.format(TickDataSourceDatabaseConf.SCHEMA, TickDataSourceDatabaseConf.TABLE,
                   TickDataSourceDatabaseConf.STOCK_CODE_COL_NAME, stock_code)

        conn = self.db_engine.connect()
        try:
            count = pd.read_sql(sql, con=conn)['stock_count'][0]
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            conn.close()

        return Error.SUCCESS, count > 0

    def get_tick_data_from_old_db_on_day(self, stock_code, day):
        """
        Implementation of "get_tick_data_from_data_source".
        :param stock_code: stock code
        :param day: specify a day to fetch tick data
        :return: err_code, a dataframe contains tick data on "day"
        """

        sql = """
            SELECT * FROM "{0}"."{1}"
            WHERE {2}='{3}' AND "date"='{4}'
        """.format(TickDataSourceDatabaseConf.SCHEMA, TickDataSourceDatabaseConf.TABLE,
                   TickDataSourceDatabaseConf.STOCK_CODE_COL_NAME, stock_code, day)

        conn = self.db_engine.connect()
        try:
            df = pd.read_sql(sql, con=conn)
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            conn.close()

        if df.shape[0] == 0:
            return Error.ERROR_TICK_DATA_NOT_EXISTS_IN_OLD_DB_ON_DAY, None

        return Error.SUCCESS, df
