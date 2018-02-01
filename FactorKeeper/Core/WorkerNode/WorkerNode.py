from Core.DAO.FactorDao.FactorDao import FactorDao
from Core.Logger.Logger import Logger
from Core.WorkerNode.WorkerNodeImpl.MessageSender import MessageSender
from Core.Conf.WorkerConf import WorkerConf
from Core.Conf.PathConf import Path
from Core.Conf.DatabaseConf import DBConfig
from Core.WorkerNode.WorkerNodeImpl.Initializer import LogInitializer, Initializer
from Core.WorkerNode.WorkerNodeImpl.FactorUpdateManager import FactorUpdateManager
from Core.WorkerNode.WorkerNodeImpl.TickDataUpdateManager import TickDataUpdateManager
from Core.WorkerNode.WorkerNodeImpl.WorkerTaskManager import WorkerTaskManager
from Core.Error.Error import Error
import time, threading, traceback


class Worker(object):
    def __init__(self):
        # init log dir
        LogInitializer.init_log_dir()

        # init logger
        self.logger = Logger(Path.WORKERNODE_MANAGER_LOG_PATH, "WorkerNode")

        # init db engine
        # self.db_engine = DBConfig.default_config().create_default_sa_engine_without_pool()
        self.db_engine = None

        # init worker node
        self.initializer = Initializer(self.logger)
        self.initializer.init_worker_node()

        # init dao
        self.dao = FactorDao(self.db_engine, self.logger)
        self.task_manager = WorkerTaskManager(processes=WorkerConf.PROCESSOR_NUM)
        self.factor_update_manager = FactorUpdateManager(self.task_manager, self.db_engine, self.logger)
        self.tick_update_manager = TickDataUpdateManager(self.task_manager, self.db_engine, self.logger)

        # start worker node routine
        threading.Thread(target=lambda: self.routine()).start()

    def update_factor_result(self, factor, version, stock_code, task_id):
        return self.factor_update_manager.update_linkage(factor, version, stock_code, task_id)

    def update_tick_data_result(self, stock_code, task_id):
        return self.tick_update_manager.update_stock_data(stock_code, task_id)

    def query_update_status(self, task_id):
        return self.factor_update_manager.query_update_status(task_id)

    def stop_all_tasks(self):
        return self.factor_update_manager.stop_all_factor_update_task_groups()

    def stop_task(self, task_id):
        return self.factor_update_manager.stop_task_group(task_id)

    def routine(self):
        """
        1. update worker info
        :return:
        """

        while True:
            try:
                while True:
                    err, tasks = self.task_manager.get_task_group_list()
                    if err:
                        self.logger.log_error("Error({}) occurred".format(err))

                    err, msg = MessageSender.update_worker_info(WorkerConf.SERVER_HOST, WorkerConf.SERVER_PORT,
                                                                tasks,
                                                                self.logger)

                    if err:
                        if err == Error.ERROR_WORKER_NOT_EXISTS:
                            self.logger.log_info("namenode restarted, trying to register again")
                            err = self.initializer.register_worker(retry=True)
                            if not err:
                                continue

                        self.logger.log_error("Error({0}): {1}".format(err, msg))

                    time.sleep(WorkerConf.UPDATE_CYCLE)
            except:
                self.logger.log_error(traceback.format_exc())
