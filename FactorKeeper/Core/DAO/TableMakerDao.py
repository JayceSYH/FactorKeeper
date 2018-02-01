"""
    This file defines functions used to create factor keeper schemas or tables in database.
    You should not modify this file.
"""


from Core.Conf.DatabaseConf import Schemas, Tables
from Core.DAO.ComplicatedTables.TickDataTable import TickDataTable
from Core.Error.Error import Error
import traceback


class TableMaker(object):
    def __init__(self, db_engine, logger):
        self._logger = logger.sub_logger(self.__class__.__name__)
        self.db_engine = db_engine

    def create_schema_if_not_exist(self, schema):
        """
        Create a new schema
        :param schema:
        :return: err_code
        """
        create_schema_sql = """
                CREATE SCHEMA IF NOT EXISTS "{0}";
                """.format(schema)

        conn = self.db_engine.connect()
        try:
            conn.execute(create_schema_sql)
        except:
            self._logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            conn.close()

        return Error.SUCCESS

    def create_factor_list_table(self):
        """
        Create factor list table
        :return: err_code
        """
        create_factor_list_sql = """
                CREATE TABLE IF NOT EXISTS "{0}"."{1}" (
                    factor text PRIMARY KEY NOT NULL,
                    creator text,
                    maintainers text,
                    create_time timestamp without time zone NOT NULL
                )
                """.format(Schemas.SCHEMA_META, Tables.TABLE_FACTOR_LIST)

        conn = self.db_engine.connect()
        try:
            conn.execute(create_factor_list_sql)
        except:
            self._logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            conn.close()

        return Error.SUCCESS

    def create_factor_version_table(self):
        """
        Create factor version table
        :return: err_code
        """
        create_factor_version_sql = """
            CREATE TABLE IF NOT EXISTS "{0}"."{1}" (
                version_id serial PRIMARY KEY NOT NULL,
                factor text NOT NULL,
                version text NOT NULL,
                code bytea NOT NULL,
                creator text
            );
            CREATE INDEX IF NOT EXISTS factor_version_id_index ON "{0}"."{1}"(factor, version);
        """.format(Schemas.SCHEMA_META, Tables.TABLE_FACTOR_VERSION)

        conn = self.db_engine.connect()
        try:
            conn.execute(create_factor_version_sql)
        except:
            self._logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            conn.close()

        return Error.SUCCESS

    def create_factor_tick_linkage_table(self):
        """
        Create linkage table
        :return: err_code
        """
        create_factor_tick_linkage_sql = """
            CREATE TABLE IF NOT EXISTS "{0}"."{1}" (
                linkage_id serial PRIMARY KEY NOT NULL,
                version_id int NOT NULL,
                stock_code text NOT NULL,
                create_time timestamp without time zone NOT NULL,
                update_time timestamp without time zone NOT NULL
            );
            CREATE INDEX IF NOT EXISTS "factor_version_id_index_l" ON "{0}"."{1}"(version_id);
            CREATE INDEX IF NOT EXISTS "stock_code_index_l" ON "{0}"."{1}"(stock_code);
        """.format(Schemas.SCHEMA_META, Tables.TABLE_FACTOR_TICK_LINKAGE)

        conn = self.db_engine.connect()
        try:
            conn.execute(create_factor_tick_linkage_sql)
        except:
            self._logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            conn.close()

        return Error.SUCCESS

    def create_factor_update_log_table(self):
        """
        Create factor update log table
        :return: err_code
        """
        create_factor_update_log_table_sql = """
                CREATE TABLE IF NOT EXISTS "{0}"."{1}" (
                    log_id serial PRIMARY KEY NOT NULL,
                    linkage_id int NOT NULL,
                    factor_date date NOT NULL,
                    start_update_time timestamp without time zone NOT NULL,
                    end_update_time timestamp without time zone
                );
                CREATE INDEX IF NOT EXISTS "factor_update_log_index_linkage" ON "{0}"."{1}"(linkage_id);
                CREATE INDEX IF NOT EXISTS "factor_update_log_index_update_date" ON "{0}"."{1}"(factor_date);
            """.format(Schemas.SCHEMA_META, Tables.TABLE_FACTOR_UPDATE_LOG)

        conn = self.db_engine.connect()
        try:
            conn.execute(create_factor_update_log_table_sql)
        except:
            self._logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            conn.close()

        return Error.SUCCESS

    def create_factor_table(self, factor, link_id):
        """
        Create factor table.
        :param factor:
        :param link_id:
        :return: err_code
        """
        create_sql = """
            CREATE TABLE IF NOT EXISTS "{0}"."{1}{2}"(
              "datetime" timestamp without time zone NOT NULL,
              "date" date NOT NULL,
              "{3}" double precision
            );
            CREATE INDEX IF NOT EXISTS factor_table_datetime_index_{2} ON "{0}"."{1}{2}"(datetime);
            CREATE INDEX IF NOT EXISTS factor_table_date_index_{2} ON "{0}"."{1}{2}"(date);
        """.format(Schemas.SCHEMA_FACTOR_DATA, Tables.TABLE_FACTOR_RESULT_PREFIX, link_id, factor)

        conn = self.db_engine.connect()
        try:
            conn.execute(create_sql)
        except:
            self._logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            conn.close()

        return Error.SUCCESS

    def create_group_factor_table(self, factors, link_id):
        """
        create group factor table
        :param factors: sub factors
        :param link_id:
        :return: err_code
        """
        fields = ["\"{}\" double precision ".format(factor) for factor in factors]

        create_sql = """
            CREATE TABLE IF NOT EXISTS "{0}"."{1}{2}"(
              "datetime" timestamp without time zone NOT NULL,
              "date" date NOT NULL,
              {3}
            );
            CREATE INDEX IF NOT EXISTS factor_table_datetime_index_{2} ON "{0}"."{1}{2}"(datetime);
            CREATE INDEX IF NOT EXISTS factor_table_date_index_{2} ON "{0}"."{1}{2}"(date);
        """.format(Schemas.SCHEMA_FACTOR_DATA, Tables.TABLE_FACTOR_RESULT_PREFIX, link_id, ", ".join(fields))

        conn = self.db_engine.connect()
        try:
            conn.execute(create_sql)
        except:
            self._logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            conn.close()

        return Error.SUCCESS

    def create_group_factor_list_table(self):
        """
        create group factor list table
        :return: err_code
        """
        create_factor_group_table_sql = """
                        CREATE TABLE IF NOT EXISTS "{0}"."{1}" (
                            id serial PRIMARY KEY NOT NULL,
                            group_factor_name text NOT NULL,
                            sub_factor_name text NOT NULL,
                            version text NOT NULL 
                        );
                        """.format(Schemas.SCHEMA_META, Tables.TABLE_GROUP_FACTOR)

        conn = self.db_engine.connect()
        try:
            conn.execute(create_factor_group_table_sql)
        except:
            self._logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            conn.close()

    def create_tick_data_table(self, stock_code):
        """
        Create tick data table, Make sure to define tick data table structure properly first.
        :param stock_code:
        :return: err_code
        """
        create_tick_sql = """
            {4};
            CREATE INDEX IF NOT EXISTS tick_{3}_date ON "{0}"."{1}{2}"("date");
            CREATE INDEX IF NOT EXISTS tick_{3}_datetime ON "{0}"."{1}{2}"("datetime");
        """.format(Schemas.SCHEMA_TICK_DATA, Tables.TABLE_TICK_STOCK_PREFIX,
                   stock_code, stock_code.split(".")[0],
                   TickDataTable().get_table_define_sql(Schemas.SCHEMA_TICK_DATA, Tables.TABLE_TICK_STOCK_PREFIX +
                                                        stock_code))

        conn = self.db_engine.connect()
        try:
            conn.execute(create_tick_sql)
        except:
            self._logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            conn.close()

        return Error.SUCCESS

    def create_tick_update_log_table(self):
        """
        Create tick data update log table.
        :return: err_code
        """
        create_update_log_sql = """
        CREATE TABLE IF NOT EXISTS "{0}"."{1}" (
            log_id serial PRIMARY KEY NOT NULL,
            start_update_time timestamp without time zone NOT NULL,
            end_update_time timestamp without time zone,
            stock_code text NOT NULL,
            update_date date NOT NULL 
        );
        CREATE INDEX IF NOT EXISTS tick_log_stock_code_index on "{0}"."{1}"(stock_code);
        CREATE INDEX IF NOT EXISTS tick_log_update_date_index on "{0}"."{1}"(update_date);
        """.format(Schemas.SCHEMA_META, Tables.TABLE_TICK_UPDATE_LOGS)

        conn = self.db_engine.connect()
        try:
            conn.execute(create_update_log_sql)
        except:
            self._logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            conn.close()

        return Error.SUCCESS

    def create_finished_tasks_table(self):
        """
        Create finished task list table.
        :return: err_code
        """
        create_finished_task_sql = """
                CREATE TABLE IF NOT EXISTS "{0}"."{1}" (
                    id serial PRIMARY KEY NOT NULL,
                    commit_time timestamp without time zone NOT NULL,
                    finish_time timestamp without time zone NOT NULL,
                    task_id text UNIQUE NOT NULL,
                    task_type text NOT NULL,
                    total_sub_tasks integer,
                    finished integer,
                    aborted integer,
                    final_status text NOT NULL,
                    is_sub_task integer NOT NULL,
                    worker_id text NOT NULL
                );
                CREATE INDEX IF NOT EXISTS finish_task_task_type_index on "{0}"."{1}"(task_type);
                CREATE INDEX IF NOT EXISTS finish_task_commit_time_index on "{0}"."{1}"(commit_time);
                """.format(Schemas.SCHEMA_META, Tables.TABLE_MANAGER_FINISHED_TASKS)

        conn = self.db_engine.connect()
        try:
            conn.execute(create_finished_task_sql)
        except:
            self._logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            conn.close()

        return Error.SUCCESS

    def create_finish_task_dependency_table(self):
        """
        Create finished task dependency list table
        :return: err_code
        """
        create_finished_task_sql = """
                        CREATE TABLE IF NOT EXISTS "{0}"."{1}" (
                            id serial PRIMARY KEY NOT NULL,
                            base_task_id text NOT NULL ,
                            dependency_task_id text NOT NULL
                        );
                        CREATE INDEX IF NOT EXISTS finish_task_dependency_task_type_index on "{0}"."{1}"(base_task_id);
                        """.format(Schemas.SCHEMA_META, Tables.TABLE_MANAGER_FINISHED_TASK_DEPENDENCY)

        conn = self.db_engine.connect()
        try:
            conn.execute(create_finished_task_sql)
        except:
            self._logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            conn.close()

    def create_stock_view_list_table(self):
        """
        Create stock view list table
        :return: err_code
        """
        create_stock_view_list_sql = """
                        CREATE TABLE IF NOT EXISTS "{0}"."{1}" (
                            stock_view_name text NOT NULL PRIMARY KEY,
                            stock_view_relation text NOT NULL
                        );
                        """.format(Schemas.SCHEMA_META, Tables.TABLE_TICK_STOCK_VIEW_LIST)

        conn = self.db_engine.connect()
        try:
            conn.execute(create_stock_view_list_sql)
        except:
            self._logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            conn.close()

    def create_stock_view_table(self, stock_view_name, stock_relation):
        """
        Create stock view data table
        :param stock_view_name:
        :param stock_relation:
        :return: err_code
        """
        from Core.DAO.ComplicatedTables.StockViewTable import StockViewTable
        create_tick_sql = """
                    {4};
                    CREATE INDEX IF NOT EXISTS stock_view_{3}_date ON "{0}"."{1}{2}"("date");
                    CREATE INDEX IF NOT EXISTS stock_view_{3}_datetime ON "{0}"."{1}{2}"("datetime");
                """.format(Schemas.SCHEMA_STOCK_VIEW_DATA, Tables.TABLE_TICK_STOCK_VIEW_PREFIX,
                           stock_view_name, stock_view_name.split(".")[0],
                           StockViewTable(stock_relation).get_table_define_sql(Schemas.SCHEMA_STOCK_VIEW_DATA,
                                                                               Tables.TABLE_TICK_STOCK_VIEW_PREFIX +
                                                                               stock_view_name))

        conn = self.db_engine.connect()
        try:
            conn.execute(create_tick_sql)
        except:
            self._logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            conn.close()

        return Error.SUCCESS
