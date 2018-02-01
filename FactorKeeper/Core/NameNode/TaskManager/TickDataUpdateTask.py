from Core.NameNode.TaskManager.TaskManager import TaskHandler, BaseTask
from Core.Error.Error import Error
from Core.DAO.FactorDao.FactorDao import FactorDao
from Core.DAO.TickDataDao import TickDataDao
from Core.DAO.TableMakerDao import TableMaker
from Core.Conf.DatabaseConf import Schemas, Tables
from Core.Conf.TickDataConf import TickDataConf
import traceback


class TickDataUpdateTaskHandler(TaskHandler):
    TASK_TYPE = "UPDATE_TICK_DATA"

    @classmethod
    def gen_task_desc(cls, *args, **kwargs):
        stock_code = kwargs['stock_code']

        return Error.SUCCESS, "UpdateTickData$$" + stock_code

    def __init__(self, worker_manager, db_engine, logger):
        super().__init__(worker_manager, logger)
        self.logger = logger.sub_logger(self.__class__.__name__)
        self.db_engine = db_engine
        self.factor_dao = FactorDao(db_engine, logger)
        self.tick_dao = TickDataDao(db_engine, logger)
        self.table_maker = TableMaker(db_engine, logger)

    def new_task(self, *args, **kwargs):
        stock_code = kwargs['stock_code']

        err, is_stock_available = self.tick_dao.is_stock_available(stock_code)
        if err:
            return err, None

        if not TickDataConf.is_stock_view(stock_code):
            err, is_table_exists = self.tick_dao.is_stock_table_exists_in_factor_keeper_db(stock_code)
            if err:
                return err, None

            if not is_table_exists:
                err = self.table_maker.create_tick_data_table(stock_code)
                if err:
                    return err, None
        else:
            err, is_table_exists = self.tick_dao.is_stock_view_table_exists(stock_code)
            if err:
                return err, None

            if not is_table_exists:
                err, relation = self.tick_dao.get_stock_view_relation(stock_code)
                if err:
                    return err, None

                err = self.table_maker.create_stock_view_table(stock_code, relation)
                if err:
                    return err, None

        if not is_stock_available:
            return Error.ERROR_TICK_DATA_NOT_AVAILABLE, None

        _, task_desc = self.gen_task_desc(stock_code=stock_code)
        update_task = TickDataUpdateTask(task_desc, stock_code)

        if TickDataConf.is_stock_view(stock_code):
            err, relations = self.tick_dao.get_stock_view_relation(stock_code)
            if err:
                return err, None
            stocks = relations.keys()
            for dep_stock in stocks:
                err, dep_task = self.new_task(stock_code=dep_stock)
                if err:
                    return err, None

                update_task.add_dependency(dep_task)

        return Error.SUCCESS, update_task

    def start_task(self, task):
        return self._worker_manager.send_command("update_tick_data",
                                                 data={
                                                     "task_id": task.task_id,
                                                     "stock_code": task.stock_code
                                                 })

    def query_status(self, task):
        return self._worker_manager.send_command("update_tick_data/status",
                                                 data={
                                                     "task_id": task.task_id
                                                 })

    def stop_task(self, task):
        return self._worker_manager.send_command("update_tick_data/stop",
                                                 data={
                                                     "task_id": task.task_id
                                                 })

    def call_back(self, *args, **kwargs):
        stock_code = kwargs['stock_code']
        df = kwargs['data_frame']
        day = kwargs['date']

        if df.shape[0] != TickDataConf.TICK_LENGTH:
            return Error.ERROR_TICK_RESULT_INCORRECT

        err, log_id = self.tick_dao.create_new_update_log(stock_code, day)
        if err:
            return err

        conn = self.db_engine.connect()
        try:
            if not TickDataConf.is_stock_view(stock_code):
                df.to_sql(Tables.TABLE_TICK_STOCK_PREFIX + stock_code, schema=Schemas.SCHEMA_TICK_DATA,
                          if_exists='append', index=False, con=conn)
            else:
                df.to_sql(Tables.TABLE_TICK_STOCK_VIEW_PREFIX + stock_code, schema=Schemas.SCHEMA_STOCK_VIEW_DATA,
                          if_exists='append', index=False, con=conn)
        except:
            self._logger.log_error(traceback.format_exc())
            return Error.ERROR_DB_EXECUTION_FAILED
        finally:
            conn.close()

        err = self.tick_dao.finish_update_log(log_id)

        return err

    def stop_all(self):
        pass

    def list_tasks(self):
        pass


class TickDataUpdateTask(BaseTask):
    TASK_TYPE = TickDataUpdateTaskHandler.TASK_TYPE

    def __init__(self, task_desc, stock_code, worker_info=None):
        super().__init__(task_desc, worker_info)

        self.stock_code = stock_code

    def task_str(self, deps_status):
        task_str = "Task Id: {0} <br>" + \
                   "Stock Code: {1} <br>" +\
                   "Status: {2} <br>" + \
                   "Worker Id: {3} <br>" + \
                   "Dependencies: <br>" + \
                   "&nbsp" * 10 + "[{4} <br>" + \
                   "&nbsp" * 10 + "]"

        task_str = task_str.format(self.task_id, self.stock_code, self.status_desc,
                                   self.worker_info.id if self.worker_info is not None else "Not Assigned",
                                   deps_status)

        task_str = task_str.replace("<br>", "<br>|")
        task_str = "<br>" + "_" * 100 + "<br>|" + task_str + "<br>|" + "_" * 100

        return task_str
