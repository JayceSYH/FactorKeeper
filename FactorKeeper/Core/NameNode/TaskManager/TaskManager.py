from Core.DAO.MasterDao import MasterDao
from Core.Error.Error import Error
from Core.Conf.MasterConf import MasterConf
from Core.NameNode.TaskManager.CommonTask import BaseTask, FinishedTask
import threading, datetime, traceback, time


class TaskManager(object):
    """
        TaskManager is used to manage running/waiting tasks and their handlers. You need to install task handler
        before using them.
    """
    def __init__(self, worker_manager, db_engine, logger):
        self.task_handlers = {}
        self.waiting_task_table = {}
        self.running_task_table = {}
        self.master_dao = MasterDao(db_engine, logger)
        self.lock = threading.Lock()
        self.logger = logger.sub_logger(self.__class__.__name__)
        self.db_engine = db_engine
        self.worker_manager = worker_manager
        threading.Thread(target=lambda: self.routine()).start()

    def install_task_handler(self, task_handler):
        """
        install a new task handler
        :param task_handler:
        :return: err_code
        """
        if issubclass(task_handler, TaskHandler):
            if task_handler.TASK_TYPE is None:
                self.logger.log_warn("task handler installation failed: 'None:{0}' founded".
                                     format(task_handler.__class__.__name__))
            elif task_handler in self.task_handlers:
                self.logger.log_warn("task handler installation failed: handler '{0}:{1}' already installed".
                                     format(task_handler.TASK_TYPE, task_handler.__class__.__name__))
            else:
                new_handler = task_handler(self.worker_manager, self.db_engine, self.logger)
                new_handler.set_task_manager(self)
                self.task_handlers[task_handler.TASK_TYPE] = new_handler
                self.logger.log_info("task handler '{0}:{1}' installed".
                                     format(task_handler.TASK_TYPE, task_handler.__class__.__name__))

    def get_handler(self, task_handler):
        """
        get a installed handler
        :param task_handler:
        :return: err_code, handler
        """
        for task_type in self.task_handlers:
            if task_handler.TASK_TYPE == task_type:
                return Error.SUCCESS, self.task_handlers[task_type]

        return Error.ERROR_TASK_HANDLER_NOT_EXISTS, None

    def get_finished_tasks(self, recent_num):
        """
        get finished task list
        :param recent_num:
        :return: err_code, task_list
        """
        return self.master_dao.get_recent_finished_tasks(recent_num)

    def __start_task(self, task):
        return self.task_handlers[task.TASK_TYPE].start_task(task)

    def __gen_task_desc(self, task_type, *args, **kwargs):
        return self.task_handlers[task_type].gen_task_desc(*args, **kwargs)

    def __finish_task_by_desc(self, task_desc, ret_task=False, aborted=False):
        self.lock.acquire()
        try:
            if task_desc in self.running_task_table:
                task = self.running_task_table[task_desc]
                self.running_task_table.pop(task_desc)
                if not aborted:
                    task.finish_task_normally()
            elif task_desc in self.waiting_task_table:
                task = self.waiting_task_table[task_desc]
                self.waiting_task_table.pop(task_desc)
            else:
                if ret_task:
                    return Error.ERROR_TASK_NOT_EXISTS, None
                else:
                    return Error.ERROR_TASK_NOT_EXISTS
        finally:
            self.lock.release()

        if ret_task:
            return Error.SUCCESS, task
        else:
            return Error.SUCCESS

    def __add_waiting_task(self, task):
        self.lock.acquire()
        try:
            task.set_worker(None)
            self.waiting_task_table[task.task_desc] = task
        finally:
            self.lock.release()
        return Error.SUCCESS

    def __add_running_task(self, task):
        self.lock.acquire()
        try:
            self.running_task_table[task.task_desc] = task
        finally:
            self.lock.release()
        return Error.SUCCESS

    def __is_task_exists(self, task_desc):
        self.lock.acquire()
        try:
            is_exists = (task_desc in self.running_task_table) or (task_desc in self.waiting_task_table)
        finally:
            self.lock.release()
        return Error.SUCCESS, is_exists

    def __get_task(self, task_desc):
        self.lock.acquire()
        try:
            task = self.running_task_table.get(task_desc, None)
            if task is None:
                task = self.waiting_task_table.get(task_desc, None)
        finally:
            self.lock.release()

        if task is not None:
            return Error.SUCCESS, task
        else:
            return Error.ERROR_TASK_NOT_EXISTS, None

    def __reset_task_table(self):
        self.lock.acquire()
        self.waiting_task_table = {}
        self.running_task_table = {}
        self.lock.release()
        return Error.SUCCESS

    def __record_finished_task(self, task, aborted=False, finished_task_num=None, aborted_task_num=None,
                               total_task_num=None):
        if finished_task_num is not None and aborted_task_num is not None and total_task_num is not None \
                and task.worker_info is not None:
            err = self.master_dao.add_finish_task(task.task_id, task.TASK_TYPE,
                                                  task.create_time, datetime.datetime.now(),
                                                  "finished" if not aborted else "aborted",
                                                  total_tasks=total_task_num, finished_num=finished_task_num,
                                                  aborted_num=aborted_task_num, is_sub_task=task.is_sub_task(),
                                                  worker_id=task.worker_info.id)

            if err:
                return err

            dependencies = [dep_task.task_id for dep_task in task.all_dependencies]
            err = self.master_dao.add_task_dependency(task.task_id, dependencies)

            return err
        elif task.worker_info is not None:
            assert 0, "UnImplemented finish task :{}".format(__file__)

    def new_task(self, task_type, *args, **kwargs):
        """
        create a new task
        :param task_type:
        :param args:
        :param kwargs:
        :return: err_code, a new task object
        """
        if issubclass(task_type, TaskHandler):
            task_type = task_type.TASK_TYPE

        # create new task
        err, task = self.task_handlers[task_type].new_task(*args, **kwargs)
        if err:
            return err, None

        # check if task exists
        err, is_task_exists = self.__is_task_exists(task.task_desc)
        if err:
            return err, None

        if is_task_exists:
            return Error.ERROR_TASK_ALREADY_EXISTS, None

        return self.start_task(task)

    def start_task(self, task):
        """
        start a task
        :param task:
        :return: err_code, error message
        """
        # check task status and load it
        if task.status == BaseTask.STATUS_READY:
            # task is ready
            err, msg, worker_info = self.__start_task(task)
            if err == Error.ERROR_TASK_HAS_NOTHING_TO_BE_DONE:
                return Error.SUCCESS, "Task Has Nothing To Be Done"
            elif not err:
                task.set_worker(worker_info)
                self.__add_running_task(task)
                return Error.SUCCESS, msg
            elif err == Error.ERROR_NO_WORKER_TO_BE_ASSIGNED:
                task.set_worker(None)
                self.__add_waiting_task(task)
                return Error.SUCCESS, "Ready To Run"
            else:
                return err, msg

        else:
            # waiting for dependency finishing
            task.set_worker(None)
            self.__add_waiting_task(task)
            return Error.SUCCESS, "Waiting For Dependency Tasks"

    def restart_task(self, task_id):
        """
        restart a task
        :param task_id:
        :return: err_code
        """
        task_desc = BaseTask.get_desc_from_id(task_id)
        task = None

        self.lock.acquire()
        try:
            if task_desc in self.running_task_table:
                task = self.running_task_table[task_desc]
                if task.task_id == task_id:
                    self.running_task_table.pop(task_desc)
            self.logger.log_error(traceback.format_exc())
        finally:
            self.lock.release()

        if task is not None:
            err, msg = self.start_task(task)
            return err
        else:
            return Error.ERROR_TASK_NOT_EXISTS

    def query_task(self, task_type, *args, **kwargs):
        """
        query task status
        :param task_type:
        :param args:
        :param kwargs:
        :return: err_code, task_status
        """
        if issubclass(task_type, TaskHandler):
            task_type = task_type.TASK_TYPE

        err, task_desc = self.__gen_task_desc(task_type, *args, **kwargs)
        if err:
            return err, None

        err, task = self.__get_task(task_desc)
        if err:
            return err, None

        if task.status == BaseTask.STATUS_RUNNING:
            return self.task_handlers[task_type].query_status(task)
        elif task.status == BaseTask.STATUS_READY:
            return Error.SUCCESS, "task is ready to run"
        else:
            return Error.SUCCESS, "task is waiting for dependency"

    def finish_task(self, task_id, aborted=False, finished_task_num=None, aborted_task_num=None, total_task_num=None):
        """
        finish a running task and record it
        :param task_id:
        :param aborted:
        :param finished_task_num:
        :param aborted_task_num:
        :param total_task_num:
        :return: err_code
        """
        task_desc = BaseTask.get_desc_from_id(task_id)

        err, task = self.__finish_task_by_desc(task_desc, aborted=aborted, ret_task=True)
        if err:
            return err

        return self.__record_finished_task(task, aborted=aborted, finished_task_num=finished_task_num,
                                           aborted_task_num=aborted_task_num, total_task_num=total_task_num)

    def stop_task(self, task_type, *args, **kwargs):
        """
        stop a running task
        :param task_type:
        :param args:
        :param kwargs:
        :return: err_code
        """
        if issubclass(task_type, TaskHandler):
            task_type = task_type.TASK_TYPE

        err, task_desc = self.__gen_task_desc(task_type, *args, **kwargs)
        if err:
            return err, None

        err, task = self.__finish_task_by_desc(task_desc, ret_task=True, aborted=True)
        if err:
            return err, None

        if task.status in [BaseTask.STATUS_READY, BaseTask.STATUS_WAITING_DEPENDENCY]:
            return Error.SUCCESS, "task stopped"
        else:
            err, msg = self.task_handlers[task_type].stop_task(task)
            if err:
                return err, msg
            else:
                return Error.SUCCESS, "task stopped"

    def callback_task(self, task_type, *args, **kwargs):
        """
        task callback
        :param task_type:
        :param args:
        :param kwargs:
        :return: err_code
        """
        if issubclass(task_type, TaskHandler):
            task_type = task_type.TASK_TYPE

        task_id = kwargs.get("task_id")
        if task_id is None:
            return Error.ERROR_PARAMETER_MISSING_OR_INVALID

        task_desc = BaseTask.get_desc_from_id(task_id)
        err, task = self.__get_task(task_desc)
        if err:
            return err

        if task_id == task.task_id:
            return self.task_handlers[task_type].call_back(*args, **kwargs)
        else:
            return Error.ERROR_TASK_NOT_EXISTS

    def stop_all(self):
        """
        stop all tasks
        :return:
        """
        self.__reset_task_table()
        self.worker_manager.send_command("stop_all", broadcast=True)
        return Error.SUCCESS

    def list_tasks(self):
        """
        list all tasks
        :return: err_code, task list
        """
        task_list = []

        self.lock.acquire()
        try:
            for task_desc in self.running_task_table:
                task = self.running_task_table[task_desc]
                if not task.is_sub_task():
                    task_list.append(task)
            for task_desc in self.waiting_task_table:
                task = self.waiting_task_table[task_desc]
                if not task.is_sub_task():
                    task_list.append(task)
        finally:
            self.lock.release()

        return Error.SUCCESS, task_list

    def routine(self):
        """
        check weather tasks are handled properly
        :return: None
        """

        while True:
            try:
                while True:
                    reconnect_logs = []

                    self.lock.acquire()
                    try:
                        running_worker_task_mapping = {}

                        # get task list
                        for task_desc in self.running_task_table:
                            task = self.running_task_table[task_desc]
                            worker = task.worker_info
                            if worker is not None:
                                running_worker_task_mapping[worker] = running_worker_task_mapping.get(worker, []) + [task]

                        # check running tasks
                        for worker in running_worker_task_mapping:
                            if not self.worker_manager.is_alive(worker):
                                for task in running_worker_task_mapping[worker]:
                                    self.running_task_table.pop(task.task_desc)
                                    task.set_worker(None)
                                    self.waiting_task_table[task.task_desc] = task
                                    reconnect_logs.append("worker({0}:{1}) disconnected, rearrange task worker...".
                                                         format(worker.host, worker.port))

                        # check waiting tasks
                        err, is_workers_ready = self.worker_manager.is_workers_ready()
                        if not err and is_workers_ready:
                            for task_desc in list(self.waiting_task_table.keys()):
                                task = self.waiting_task_table[task_desc]
                                if task.status == BaseTask.STATUS_READY:
                                    err, _, worker = self.__start_task(task)
                                    if err == Error.ERROR_TASK_HAS_NOTHING_TO_BE_DONE:
                                        task.finish_task_normally()
                                        self.waiting_task_table.pop(task.task_desc)
                                        self.__record_finished_task(task, aborted=False, finished_task_num=0,
                                                                    aborted_task_num=0, total_task_num=0)
                                    elif not err:
                                        task.set_worker(worker)
                                        self.running_task_table[task.task_desc] = task
                                        self.waiting_task_table.pop(task.task_desc)
                                    elif err == Error.ERROR_NO_WORKER_TO_BE_ASSIGNED:
                                        break

                                elif task.status == BaseTask.STATUS_WAITING_DEPENDENCY:
                                    err, runnable_dependencies = task.get_runnable_dependencies()
                                    if not err:
                                        for dep_task in runnable_dependencies:
                                            if dep_task.status == BaseTask.STATUS_READY and \
                                                    dep_task.task_desc not in self.running_task_table:
                                                err, _, worker = self.__start_task(dep_task)
                                                if err == Error.ERROR_TASK_HAS_NOTHING_TO_BE_DONE:
                                                    dep_task.finish_task_normally()
                                                    self.__record_finished_task(dep_task, aborted=False,
                                                                                finished_task_num=0,
                                                                                aborted_task_num=0, total_task_num=0)
                                                elif not err:
                                                    dep_task.set_worker(worker)
                                                    self.running_task_table[dep_task.task_desc] = dep_task
                                                elif err == Error.ERROR_NO_WORKER_TO_BE_ASSIGNED:
                                                    break
                                        if err == Error.ERROR_NO_WORKER_TO_BE_ASSIGNED:
                                            break

                    except:
                        self.logger.log_error(traceback.format_exc())
                    finally:
                        self.lock.release()

                    for log in reconnect_logs:
                        self.logger.log_info(log)

                    time.sleep(MasterConf.TASK_CHECK_CYCLE)
            except:
                self.logger.log_error(traceback.format_exc())


class TaskHandler(object):
    """
    This class defines task handler interface, you need to implement it
    if you want to create a new task type.
    """
    TASK_TYPE = None

    def __init__(self, worker_manager, logger):
        self._worker_manager = worker_manager
        self._logger = logger
        self.__task_manager = None

    def set_task_manager(self, task_manager):
        self.__task_manager = task_manager
        return Error.SUCCESS

    def get_task_manager(self):
        return self.__task_manager

    def new_task(self, *args, **kwargs):
        """
        create a new task
        :param args:
        :param kwargs:
        :return: err_code, task object
        """
        pass

    def start_task(self, task):
        """
        start a task
        :param task:
        :return: err_code
        """
        pass

    def query_status(self, task):
        """
        query task status
        :param task:
        :return: err_code, task_status
        """
        pass

    def stop_task(self, task):
        """
        stop a task
        :param task:
        :return: err_code
        """
        pass

    def call_back(self, *args, **kwargs):
        """
        task callback
        :param args:
        :param kwargs:
        :return: err_code
        """
        pass

    @classmethod
    def gen_task_desc(cls, *args, **kwargs):
        """
        generate task description string
        :param args:
        :param kwargs:
        :return: err_code, task description string
        """
        pass
