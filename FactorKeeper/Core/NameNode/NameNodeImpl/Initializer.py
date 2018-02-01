from Core.DAO.FactorDao.FactorDao import FactorDao
from Core.DAO.TableMakerDao import TableMaker
from Core.Conf.DatabaseConf import Schemas
from Core.Error.Error import Error
import traceback


class Initializer(object):
    """
        This class implement the interface of factor manager's initialization
    """

    def __init__(self, db_engine, logger):
        self._dao = FactorDao(db_engine, logger)
        self._table_maker = TableMaker(db_engine, logger)
        self._logger = logger.sub_logger(self.__class__.__name__)

    def _check_runtime_environment(self):
        pass

    def create_schemas(self):
        err = self._table_maker.create_schema_if_not_exist(Schemas.SCHEMA_META)
        if err:
            return err

        err = self._table_maker.create_schema_if_not_exist(Schemas.SCHEMA_FACTOR_DATA)
        if err:
            return err

        err = self._table_maker.create_schema_if_not_exist(Schemas.SCHEMA_TICK_DATA)
        if err:
            return err

        err = self._table_maker.create_schema_if_not_exist(Schemas.SCHEMA_STOCK_VIEW_DATA)
        if err:
            return err

        return Error.SUCCESS

    def create_factor_tables(self):
        err = self._table_maker.create_factor_list_table()
        if err:
            return err

        err = self._table_maker.create_factor_version_table()
        if err:
            return err

        err = self._table_maker.create_factor_tick_linkage_table()
        if err:
            return err

        err = self._table_maker.create_factor_update_log_table()
        if err:
            return err

        err = self._table_maker.create_group_factor_list_table()
        if err:
            return err

        return Error.SUCCESS

    def create_tick_tables(self):
        err = self._table_maker.create_tick_update_log_table()
        if err:
            return err

        err = self._table_maker.create_stock_view_list_table()
        if err:
            return err

        return Error.SUCCESS

    def create_name_node_tables(self):
        err = self._table_maker.create_finished_tasks_table()
        if err:
            return err

        err = self._table_maker.create_finish_task_dependency_table()
        if err:
            return err

        return Error.SUCCESS

    @staticmethod
    def create_factor_generator_dir():
        """
        创建factor生成脚本根目录
        :return:
        """
        try:
            import sys, os
            from Core.Conf.PathConf import Path
            sys.path.append(Path.FACTOR_GENERATOR_BASE)
            sys.path.append(Path.SKYECON_BASE)
            os.mkdir(Path.FACTOR_GENERATOR_BASE)
        except:
            pass

    def init_master_node(self):
        self._logger.log_info("initializing name node...")
        # check runtime environment
        self._check_runtime_environment()

        # create schemas and tables
        err = self.create_schemas()
        if err:
            print("Failed to init schams, aborted")
            exit(-1)

        # create factor tables
        err = self.create_factor_tables()
        if err:
            print("Failed to init factor tables, aborted")
            exit(-1)

        # create tick tables
        err = self.create_tick_tables()
        if err:
            print("Failed to init tick tables, aborted")
            exit(-1)

        # create manager tables
        err = self.create_name_node_tables()
        if err:
            print("Failed to init name node tables, aborted")
            exit(-1)

        # create factor generator path
        self.create_factor_generator_dir()
        self._logger.log_info("successfully initialized name node.")


class LogInitializer(object):
    @staticmethod
    def init_log_dir():
        import os
        assert os.path.exists("../Log")

        try:
            if not os.path.exists("../Log/NameNode"):
                os.mkdir("../Log/NameNode")
            log_dirs = ['error', 'warn', 'info']
            for ld in log_dirs:
                if not os.path.exists('../Log/NameNode/' + ld):
                    os.mkdir('../Log/NameNode/' + ld)
        except:
            traceback.print_exc()
            exit()
