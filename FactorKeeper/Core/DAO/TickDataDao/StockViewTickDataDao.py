"""
    This file defines interfaces used to create/query/fetch stock view.
    Stock view is an abstract view of stock data, you can regard it analogous
    to view in database.

    You need to specify a stock view name and database relation to define a stock view.
    Stock view name must end with ".VIEW" and a stock view relation is always defined by
    a python dictionary like

        {
            "table1": ["col1", "col2", ...],
            "table2": ["col3", "col4", ...],
            ...
        }
"""


from Core.DAO.ComplicatedTables.TickDataTable import TickDataTable
from Core.Conf.DatabaseConf import Schemas, Tables
from Core.Conf.TickDataConf import TickDataConf
from Core.DAO.TickDataDao.TickDataImportDao import TickDataImportDao
from Core.Error.Error import Error
import traceback
import pandas as pd


class StockViewTickDataDao(object):
    def __init__(self, db_engine, logger):
        """
        :param db_engine: sqlalchemy database engine
        :param logger:
        """
        self.db_engine = db_engine
        self.logger = logger.sub_logger(self.__class__.__name__)
        self.tick_data_import_dao = TickDataImportDao(db_engine, logger)

    def create_stock_view(self, stock_view_name, stock_relation):
        """
        create a stock view with name "stock_view_name" and table relation "stock_relation".
        stock_relation should be defined as a dict:
            {
                "table1": ["col1", "col2", ...],
                "table2": ["col3", "col4", ...],
                ...
            }
        :param stock_view_name: stock view name
        :param stock_relation: stock view relation
        :return: err_code, error message
        """
        import json

        err, is_stock_view_exists = self.is_stock_view_exists(stock_view_name)
        if err:
            return err

        if is_stock_view_exists:
            return Error.ERROR_TICK_STOCK_VIEW_ALREADY_EXISTS, None

        if not isinstance(stock_relation, dict):
            return Error.ERROR_INVALID_STOCK_VIEW_RELATION, "stock relation must be a dict, but {} found". \
                format(stock_relation.__class__)

        if len(stock_relation) == 0:
            return Error.ERROR_INVALID_STOCK_VIEW_RELATION, "empty stock relation"

        needed_columns = set()
        needed_stocks = set()

        # check relation validation
        for relation_stock in list(stock_relation.keys()):
            if not isinstance(relation_stock, str):
                return Error.ERROR_INVALID_STOCK_VIEW_RELATION, "stock name must be string, but {0} found: {1}". \
                    format(relation_stock.__class__, relation_stock)
            else:
                needed_stocks.add(relation_stock)

            if TickDataConf.is_stock_view(relation_stock):
                return Error.ERROR_INVALID_STOCK_VIEW_RELATION, "stock relation cannot contain stock views"

            relation_columns = stock_relation[relation_stock]
            if not isinstance(relation_columns, str):
                if not isinstance(relation_columns, list):
                    return Error.ERROR_INVALID_STOCK_VIEW_RELATION, "columns must be string or list, but {0} found: {1}". \
                        format(relation_columns.__class__, relation_columns)
                else:
                    for col in relation_columns:
                        if not isinstance(col, str):
                            return Error.ERROR_INVALID_STOCK_VIEW_RELATION, "column name must be string but {0} found: {1}". \
                                format(col.__class__, col)
                        else:
                            needed_columns.add(col)
            else:
                needed_columns.add(relation_columns)
                stock_relation[relation_stock] = [relation_columns]

        # check if column exists
        stock_cols = set(TickDataTable().get_column_name_list(without_quote=True))
        print(stock_cols)
        if not stock_cols.issuperset(needed_columns):
            return Error.ERROR_INVALID_STOCK_VIEW_RELATION, "columns not exist in stocks, but {} found". \
                format(list(needed_columns - stock_cols))

        # check is stock exists
        for stock in needed_stocks:
            err, is_stock_exists = self.tick_data_import_dao.is_stock_available(stock)
            if err:
                return err, None

            if not is_stock_exists:
                return Error.ERROR_TICK_STOCK_NOT_EXISTS, "stock not exists: {}".format(stock)

        # create relation
        relation_json = json.dumps(stock_relation)

        insert_sql = """
            INSERT INTO "{0}"."{1}"(stock_view_name, stock_view_relation)
            VALUES(%s, %s)
        """.format(Schemas.SCHEMA_META, Tables.TABLE_TICK_STOCK_VIEW_LIST)

        conn = self.db_engine.connect()
        try:
            conn.execute(insert_sql, (stock_view_name, relation_json))
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            conn.close()

        return Error.SUCCESS, None

    def get_stock_view_relation(self, stock_view_name):
        """
        Fetch stock view relation which returned as a dict
        :param stock_view_name: stock view name
        :return: err_code, stock view relation
        """
        import json

        conn = self.db_engine.connect()
        try:
            stock_view_relation_df = pd.read_sql("""
                SELECT stock_view_relation FROM "{0}"."{1}"
                WHERE stock_view_name='{2}'
            """.format(Schemas.SCHEMA_META, Tables.TABLE_TICK_STOCK_VIEW_LIST, stock_view_name), con=conn)

            if stock_view_relation_df.shape[0] == 0:
                return Error.ERROR_TICK_STOCK_VIEW_NOT_EXISTS, None

            relation_json = stock_view_relation_df['stock_view_relation'][0]
            relation_dict = json.loads(relation_json)
            return Error.SUCCESS, relation_dict
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            conn.close()

    def is_stock_view_exists(self, stock_view_name):
        """
        Check weather stock view exists.
        :param stock_view_name: stock view name
        :return: err_code, True if stock view exists else False
        """

        conn = self.db_engine.connect()
        try:
            stock_view_relation_df = pd.read_sql("""
                        SELECT count(1) as stock_count FROM "{0}"."{1}"
                        WHERE stock_view_name='{2}'
                    """.format(Schemas.SCHEMA_META, Tables.TABLE_TICK_STOCK_VIEW_LIST, stock_view_name), con=conn)

            return Error.SUCCESS, stock_view_relation_df['stock_count'][0] > 0
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            conn.close()

    def get_stock_view_available_dates(self, stock_view_name):
        """
        Fetch list of dates when stock view data exists
        :param stock_view_name: stock view name
        :return: err_code, list of dates
        """
        err, is_stock_view_exists = self.is_stock_view_exists(stock_view_name)
        if err:
            return err, None

        if not is_stock_view_exists:
            return Error.ERROR_TICK_STOCK_VIEW_NOT_EXISTS, None

        err, stock_view_relation = self.get_stock_view_relation(stock_view_name)
        if err:
            return err, None

        stocks = stock_view_relation.keys()
        available_dates = None
        for stock in stocks:
            err, dates = self.tick_data_import_dao.get_tick_dates_from_data_source(stock)
            if err:
                return err, None

            dates = set(dates)

            if available_dates is None:
                available_dates = dates
            else:
                available_dates.intersection_update(dates)

        available_dates = sorted(list(available_dates))

        return Error.SUCCESS, available_dates

    def load_stock_view_data(self, stock_view_name, day):
        """
        Load stock view data from factor keeper database.
        :param stock_view_name: stock view name
        :param day: the day to fetch
        :return: err_code, a dataframe contains stock view tick data
        """

        conn = self.db_engine.connect()
        try:
            stock_view_df = pd.read_sql("""
                                SELECT * FROM "{0}"."{1}"
                                WHERE "date"='{2}'
                            """.format(Schemas.SCHEMA_STOCK_VIEW_DATA, Tables.TABLE_TICK_STOCK_VIEW_PREFIX + stock_view_name,
                                       day), con=conn)

            return Error.SUCCESS, stock_view_df
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            conn.close()

    def create_stock_view_index_if_not_exists(self, stock_view_name):
        """
        create stock view index if not exists
        :param stock_view_name: stock view name
        :return: err_code
        """
        conn = self.db_engine.connect()
        try:
            conn.execute("""
                CREATE INDEX IF NOT EXISTS stock_view_data_datetime_index_{2} ON "{0}"."{1}"(datetime)
            """.format(Schemas.SCHEMA_STOCK_VIEW_DATA, Tables.TABLE_TICK_STOCK_VIEW_PREFIX + stock_view_name,
                       stock_view_name.replace(".", "_")))
        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            conn.close()

        return Error.SUCCESS

    def is_stock_view_table_exists(self, stock_view_name):
        """
        Check weather stock view table exists
        :param stock_view_name: stock view name
        :return:
        """

        conn = self.db_engine.connect()
        try:
            num = pd.read_sql("""
                        SELECT count(1) as num FROM pg_tables
                        WHERE schemaname='{0}' and tablename='{1}'
                    """.format(Schemas.SCHEMA_STOCK_VIEW_DATA, Tables.TABLE_TICK_STOCK_VIEW_PREFIX + stock_view_name),
                              con=conn)['num'][0]

            if num > 0:
                return Error.SUCCESS, True
            else:
                return Error.SUCCESS, False

        except:
            self.logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED, None
        finally:
            conn.close()
