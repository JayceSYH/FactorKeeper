# ========================== Database Connection Configuration ====================================
class DBConfig(object):
    def __init__(self, host, port, user, password, db_name, db_type):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._db_type = db_type
        self._db_name = db_name

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def user(self):
        return self._user

    @property
    def password(self):
        return self._password

    @property
    def db_type(self):
        return self._db_type

    @property
    def db_name(self):
        return self._db_name

    @staticmethod
    def default_config():
        return DBConfig("localhost", 6543, "user", "passwd", "database", "db_type")

    def __create_psycopg2_connection(self):
        import psycopg2
        return psycopg2.connect(database=self.db_name, user=self.user, password=self.password,
                                host=self.host, port=self.port)

    def __create_sqlalchemy_engine(self, use_pool, pool_size=None, max_overflow=None):
        import sqlalchemy as sa
        from sqlalchemy.pool import QueuePool, NullPool
        if use_pool:
            return sa.create_engine(
                '{0}://{1}:{2}@{3}:{4}/{5}'.
                    format(self.db_type, self.user, self.password,
                           self.host, self.port, self.db_name),
                poolclass=QueuePool, pool_size=pool_size, max_overflow=max_overflow)
        else:
            return sa.create_engine(
                '{0}://{1}:{2}@{3}:{4}/{5}'.
                    format(self.db_type, self.user, self.password,
                           self.host, self.port, self.db_name),
                poolclass=NullPool)

    @classmethod
    def create_default_sa_engine(cls, use_pool=True, pool_size=5, max_overflow=5):
        return cls.default_config().__create_sqlalchemy_engine(use_pool=use_pool, pool_size=pool_size,
                                                               max_overflow=max_overflow)

    @classmethod
    def create_default_sa_engine_without_pool(cls):
        return cls.default_config().__create_sqlalchemy_engine(use_pool=False)


# ========================== Database Schema/Table Configuration ==================================
# Schemas configuration
class Schemas(object):
    SCHEMA_META = "FACTOR_KEEPER_META"
    SCHEMA_FACTOR_DATA = "FACTOR_KEEPER_FACTORDATA"
    SCHEMA_TICK_DATA = "FACTOR_KEEPER_TICKDATA"
    SCHEMA_STOCK_VIEW_DATA = "FACTOR_KEEPER_VIEWDATA"


# Table configuration
class Tables(object):
    # factor relative tables
    TABLE_FACTOR_LIST = "T_FACTOR_LIST"
    TABLE_FACTOR_VERSION = "T_FACTOR_VERSION"
    TABLE_FACTOR_TICK_LINKAGE = "T_FACTOR_TICK_LINKAGE"
    TABLE_FACTOR_RESULT_PREFIX = "T_FACTOR_RESULT_"
    TABLE_FACTOR_UPDATE_LOG = "T_FACTOR_UPDATE_LOG"
    TABLE_GROUP_FACTOR = "T_GROUP_FACTOR"

    # tick data relative tables
    TABLE_TICK_STOCK_PREFIX = "T_STOCK_"
    TABLE_TICK_STOCK_VIEW_PREFIX = "T_VIEW_"
    TABLE_TICK_UPDATE_LOGS = "T_TICK_UPDATE_LOGS"
    TABLE_TICK_STOCK_VIEW_LIST = "T_STOCK_VIEW_LIST"

    # name node relative table
    TABLE_MANAGER_FINISHED_TASKS = "T_FINISHED_TASKS"
    TABLE_MANAGER_FINISHED_TASK_DEPENDENCY = "T_TASK_DEPENDENCY"


# ========================== Tick Data Source Database Configuration ==================================
class TickDataSourceDatabaseConf(object):
    SCHEMA = "schema"
    TABLE = "table"
    STOCK_CODE_COL_NAME = "col"

    @staticmethod
    def create_db_engine():
        return DBConfig.default_config().create_default_sa_engine_without_pool()
