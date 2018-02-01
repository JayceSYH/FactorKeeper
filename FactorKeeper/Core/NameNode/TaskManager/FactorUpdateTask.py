from Core.NameNode.TaskManager.TaskManager import TaskHandler, BaseTask
from Core.NameNode.TaskManager.TickDataUpdateTask import TickDataUpdateTaskHandler
from Core.Conf.DatabaseConf import Schemas, Tables
from Core.Error.Error import Error
from Core.DAO.FactorDao.FactorDao import FactorDao
from Core.DAO.TickDataDao import TickDataDao
from Core.DAO.TableMakerDao import TableMaker
from Core.Conf.FactorConf import FactorConf
import traceback


class UpdateFactorTaskHandler(TaskHandler):
    """
        Task handler of factor update task.
    """
    TASK_TYPE = "UPDATE_FACTOR"

    def __init__(self, worker_manager, db_engine, logger):
        super().__init__(worker_manager, logger)
        self.db_engine = db_engine
        self.factor_dao = FactorDao(db_engine, logger)
        self.tick_dao = TickDataDao(db_engine, logger)
        self.table_maker = TableMaker(db_engine, logger)

    @classmethod
    def gen_task_desc(cls, *args, **kwargs):
        factor = kwargs['factor']
        version = kwargs['version']
        stock_code = kwargs['stock_code']
        return Error.SUCCESS, "UpdateFactor$${0}$${1}$${2}".format(factor, version, stock_code)

    def new_task(self, *args, **kwargs):
        factor = kwargs['factor']
        version = kwargs['version']
        stock_code = kwargs['stock_code']

        err, group_factor = self.factor_dao.get_group_factor(factor, factor)
        if err:
            return err

        # check weather linkage exists
        err, link_id = self.factor_dao.get_linkage_id(group_factor, version, stock_code)
        if err:
            return err, None

        # check if table exists
        err, is_table_exists = self.factor_dao.is_factor_table_exists(link_id)
        if err:
            return err, None

        if not is_table_exists:
            if group_factor == factor:
                err = self.table_maker.create_factor_table(factor, link_id)
                if err:
                    return err, None
            else:
                err, factors = self.factor_dao.get_sub_factors(group_factor, version)
                if err:
                    return err, None

                err = self.table_maker.create_group_factor_table(factors, link_id)
                if err:
                    return err, None

        factor = group_factor

        # check task
        err, is_newest_version = self.tick_dao.is_tick_data_newest_version(stock_code)
        if err:
            return err, None

        if is_newest_version:
            # stock already exists, just update factor
            err, task_desc = self.gen_task_desc(factor=factor, version=version, stock_code=stock_code)
            return Error.SUCCESS, UpdateFactorTask(task_desc, factor, stock_code, version, None)
        else:
            # stock not exists, update tick data first
            err, tick_update_handler = self.get_task_manager().get_handler(TickDataUpdateTaskHandler)
            if err:
                print("Tick update handler not registered ?")
                return Error.ERROR_SERVER_INTERNAL_ERROR, None
            err, tick_update_task = tick_update_handler.new_task(stock_code=stock_code)
            if err:
                return err, None

            _, task_desc = self.gen_task_desc(factor=factor, version=version, stock_code=stock_code)
            update_factor_task = UpdateFactorTask(task_desc, factor, stock_code, version, None)
            update_factor_task.add_dependency(tick_update_task)
            return Error.SUCCESS, update_factor_task

    def start_task(self, task):
        return self._worker_manager.send_command("update_factor",
                                                 data={'task_id': task.task_id,
                                                       'factor': task.factor,
                                                       'version': task.version,
                                                       'stock_code': task.stock_code})

    def query_status(self, task):
        command = "update_factor/status"
        err, status, worker_info = self._worker_manager.send_command(command, method="POST",
                                                                     worker=task.worker_info,
                                                                     data={"task_id": task.task_id})

        return err, status

    def stop_task(self, task):
        command = "update_factor/stop"
        err, status, worker_info = self._worker_manager.send_command(command, method="POST",
                                                                     worker=task.worker_info,
                                                                     data={"task_id": task.task_id})

        return err, status

    def call_back(self, *args, **kwargs):
        factor = kwargs['factor']
        version = kwargs['version']
        stock_code = kwargs['stock_code']
        df = kwargs['data_frame']
        day = kwargs['date']

        # check result data frame format
        if df.shape[0] != FactorConf.FACTOR_LENGTH:
            return Error.ERROR_INVALID_FACTOR_RESULT

        df.info()

        err, is_group_factor = self.factor_dao.is_group_factor(factor)
        if err:
            return err

        if is_group_factor:
            err, sub_factors = self.factor_dao.get_sub_factors(factor, version)
            if err:
                return err
            data_columns = set(df.columns) - {"datetime", "date"}
            if data_columns != set(sub_factors):
                return Error.ERROR_GROUP_FACTOR_SIGNATURE_NOT_MATCHED

        # save result
        err, link_id = self.factor_dao.get_linkage_id(factor, version, stock_code)
        err = self.factor_dao.clean_old_factor_data(factor, version, stock_code, day.date())
        if err:
            return err

        try:

            err, log_id = self.factor_dao.start_update_log(link_id, day.date())
            if err:
                self._logger.log_error("failed to start update log")
                return Error.ERROR_DB_EXECUTION_FAILED

            conn = self.db_engine.connect()
            try:
                df.to_sql(Tables.TABLE_FACTOR_RESULT_PREFIX + str(link_id), schema=Schemas.SCHEMA_FACTOR_DATA,
                          if_exists='append', index=False, con=conn)
            except:
                self._logger.log_error(traceback.format_exc())
                return Error.ERROR_DB_EXECUTION_FAILED
            finally:
                conn.close()

            err = self.factor_dao.finish_update_log(log_id)
            if err:
                self._logger.log_error("failed to finish update log")
                return err

            self._logger.log_info("successfully update factor to database({0}:{1}:{2})".
                                  format(factor, version, stock_code))
            return Error.SUCCESS

        except:
            self._logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED


class UpdateFactorTask(BaseTask):
    """
        Used to identify a factor update task
    """
    TASK_TYPE = UpdateFactorTaskHandler.TASK_TYPE

    def __init__(self, task_desc, factor, stock_code, version, worker_info):
        super().__init__(task_desc, worker_info)
        self.factor = factor
        self.stock_code = stock_code
        self.version = version

    def task_str(self, deps_status):
        task_str = "Task Id: {0} <br>" +\
                "Factor: {1} <br>" +\
                "Version: {2} <br>" +\
                "Stock Code: {3} <br>" +\
                "Status: {4} <br>" +\
                "Worker Id: {5} <br>" +\
                "Dependencies: <br>" +\
                "&nbsp" * 10 + "[{6} <br>" +\
                "&nbsp" * 10 + "]"

        task_str = task_str.format(self.task_id, self.factor, self.version, self.stock_code,
                                   self.status_desc,
                                   self.worker_info.id if self.worker_info is not None else "Not Assigned",
                                   deps_status)

        task_str = task_str.replace("<br>", "<br>|")
        task_str = "<br>" + "_" * 100 + "<br>|" + task_str + "<br>|" + "_" * 100

        return task_str
