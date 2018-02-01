from Core.WorkerNode.WorkerNodeImpl.Message import MessageConst
from Core.WorkerNode.WorkerNodeImpl.MessageSender import MessageSender
from Core.Error.Error import Error
from Core.Logger.Logger import TSLogger
from Core.Conf.PathConf import Path
import multiprocessing
import threading
import traceback


class Task(object):
    """
        A unit task to be executed.
    """
    def __init__(self, task_type, task_sub_id):
        self._task_type = task_type
        self._task_sub_id = task_sub_id
        self.target_task = None
        self.args = None
        self.kwargs = None
        self.callback = None
        self.progress = 0
        self.group = None
        self.message_handler = None

    def set_target(self, func, args=None, kwargs=None, call_back=None):
        self.target_task = func
        self.args = args if args is not None else ()
        self.kwargs = kwargs if kwargs is not None else {}
        self.callback = call_back

    def set_message_handler(self, message_handler):
        self.message_handler = message_handler

    @property
    def task_id(self):
        return self.make_task_id(self._task_type, self._task_sub_id)

    @staticmethod
    def make_task_id(task_type, task_sub_id):
        task_sub_id = str(task_sub_id).replace(" ", "_")
        return "{0}_{1}".format(task_type, task_sub_id)


class TaskGroup(object):
    """
        A task group contains a group of task which is split from
        an origin task by date.

        You may implement message handler to handle messages sent by
        worker process when executing unit task.
    """
    def __init__(self, task_type, group_id):
        self.task_type = task_type
        self.group_id = group_id
        self.task_num = 0
        self.running_tasks = {}
        self.finished_tasks = {}
        self.aborted_tasks = {}
        self.progress = 0
        self.message_handler = self.default_message_handler
        self.lock = threading.Lock()

    def add_task(self, task):
        self.lock.acquire()
        task.group = self
        self.running_tasks[task.task_id] = task
        self.task_num += 1
        self.lock.release()

    def abort_task(self, task):
        self.lock.acquire()
        if task.task_id in self.running_tasks:
            del self.running_tasks[task.task_id]
            self.aborted_tasks[task.task_id] = task
        self.lock.release()

    def is_finished(self):
        return len(self.running_tasks) == 0

    def default_message_handler(self, message, manager=None):
        if message.type == MessageConst.MessageType.FINISH:
            self.lock.acquire()
            if message.task_id in self.running_tasks:
                task = self.running_tasks[message.task_id]
                del self.running_tasks[message.task_id]
                if not message.aborted:
                    self.finished_tasks[message.task_id] = task
                else:
                    self.aborted_tasks[message.task_id] = task
                self.progress += 1
            self.lock.release()

            if len(self.running_tasks) == 0:
                if manager:
                    manager.remove_task_group(self.group_id)


class WorkerTaskManager(threading.Thread):
    """
        WorkerTaskManager manage all unit tasks and task groups.
        You should not modify this class.
    """
    def __init__(self, processes=1):
        super(WorkerTaskManager, self).__init__()
        self.__process_num = processes
        self.__lock = threading.Lock()
        self.__pool = multiprocessing.Pool(processes=processes)
        self.__queue = multiprocessing.Manager().Queue()
        self.logger = TSLogger(Path.WORKERNODE_WORKER_LOG_PATH, "Workers")

        self.__tasks = {}
        self.__groups = {}

    def apply_task(self, task, group_id=None):
        self.logger.log_info("apply task:{}".format(task.task_id))
        kwargs = task.kwargs.copy()
        kwargs[TaskConst.TaskParam.TASK_MANAGER_QUEUE] = self.__queue
        kwargs[TaskConst.TaskParam.TASK_ID] = task.task_id
        kwargs[TaskConst.TaskParam.LOG_STACK] = "WORK_RUNNER"
        if group_id is not None:
            kwargs[TaskConst.TaskParam.TASK_GROUP_ID] = group_id

        self.__lock.acquire()
        if task.task_id in self.__tasks:
            self.__lock.release()
            return Error.ERROR_TASK_ALREADY_EXISTS
        self.__tasks[task.task_id] = task
        self.__pool.apply_async(task.target_task, args=task.args, kwds=kwargs, callback=task.callback)
        self.__lock.release()

        return Error.SUCCESS

    def apply_task_group(self, task_group):
        if len(task_group.running_tasks) == 0:
            return Error.ERROR_TASK_GROUP_IS_EMPTY

        self.__lock.acquire()
        if task_group.group_id in self.__groups:
            self.__lock.release()
            return Error.ERROR_TASK_ALREADY_EXISTS
        self.__groups[task_group.group_id] = task_group
        self.__lock.release()

        for task_id in task_group.running_tasks:
            task = task_group.running_tasks[task_id]
            err = self.apply_task(task, task_group.group_id)
            if err:
                task_group.abort_task(task)

        if task_group.is_finished():
            self.remove_task_group(task_group.group_id)

        return Error.SUCCESS

    def get_task_group_list(self):
        group_list = []
        self.__lock.acquire()
        try:
            for group_id in self.__groups:
                group_list.append(group_id)
        finally:
            self.__lock.release()

        return Error.SUCCESS, group_list

    def message_handler(self, message):
        if message.target_type != MessageConst.MessageTargetType.TARGET_TYPE_MANAGER:
            if message.target_type == MessageConst.MessageTargetType.TARGET_TYPE_TASK:
                task = self.__tasks.get(message.target, None)
                if task is None:
                    self.logger.log_error("Message target {} not exists".format(message.target))
                    return Error.ERROR_MESSAGE_TARGET_NOT_EXISTS

                if hasattr(task, "message_handler"):
                    task.message_handler(message)
            elif message.target_type == MessageConst.MessageTargetType.TARGET_TYPE_GROUP:
                group = self.__tasks.get(message.target, None)
                if group is None:
                    self.logger.log_error("Message target {} not exists".format(message.target))
                    return Error.ERROR_MESSAGE_TARGET_NOT_EXISTS

                if hasattr(group, "message_handler"):
                    group.message_handler(message, manager=self)
                else:
                    group.default_message_handler(message, manager=self)

        else:
            return self.default_message_handler(message)

    def default_message_handler(self, message):
        if message.type == MessageConst.MessageType.PROGRESS_METRIC:
            self.__lock.acquire()
            try:
                task = self.__tasks.get(message.task_id, None)
            finally:
                self.__lock.release()
            if task is not None:
                task.progress = message.content
                self.logger.log_info("task:{0} progress:{1}".format(message.task_id, task.progress))
            else:
                self.logger.log_error("invalid progress metric message format:{}".format(message.content))
                return Error.ERROR_MESSAGE_DESERIALIZE_FAILED

        elif message.type == MessageConst.MessageType.FINISH:
            if message.task_id in self.__tasks:
                task = self.__tasks[message.task_id]

                self.__lock.acquire()
                try:
                    if message.task_id in self.__tasks:
                        self.__tasks.pop(message.task_id)
                finally:
                    self.__lock.release()

                if task.group is not None:
                    task.group.message_handler(message, manager=self)
                    task.group = None
            self.logger.log_info("task '{}' finished".format(message.task_id))

        elif message.type == MessageConst.MessageType.LOG:
            if message.log_level == "ERROR":
                self.logger.log_error(message.log_content, log_stack=message.log_stack)
            elif message.log_level == "WARN":
                self.logger.log_warn(message.log_content, log_stack=message.log_stack)
            elif message.log_level == "INFO":
                self.logger.log_info(message.log_content, log_stack=message.log_stack)

        elif message.type == MessageConst.MessageType.KILL:
            if message.task_id in self.__tasks:
                task = self.__tasks[message.task_id]
                self.remove_task_group(task.group.group_id)
                self.kill_all(restart_groups=True)

        return Error.SUCCESS

    def query_task_progress(self, task_type, task_sub_id):
        task_id = Task.make_task_id(task_type, task_sub_id)
        if task_id not in self.__tasks:
            return None, Error.ERROR_TASK_NOT_EXISTS

        task = self.__tasks[task_id]
        return task.progress, Error.SUCCESS

    def query_group_progress(self, group_id):
        if group_id not in self.__groups:
            return Error.ERROR_TASK_GROUP_NOT_EXISTS, None

        group = self.__groups[group_id]
        return Error.SUCCESS, {"progress": group.progress, "total_tasks": group.task_num,
                               "aborted_num": len(group.aborted_tasks), "finished_num": len(group.finished_tasks),
                               "finish_ratio": float(group.progress) / group.task_num}

    def stop_task_group(self, group_id):
        err = self.remove_task_group(group_id)
        if err:
            return Error.ERROR_TASK_GROUP_NOT_EXISTS
        else:
            self.kill_all(restart_groups=True)
            return Error.SUCCESS

    def stop_task_groups(self, task_type=None):
        if task_type is not None:
            self.__lock.acquire()
            try:
                factor_group_list = []
                for group_id in self.__groups:
                    if self.__groups[group_id].task_type == task_type:
                        factor_group_list.append(group_id)
            finally:
                self.__lock.release()

            for group_id in factor_group_list:
                self.remove_task_group(group_id)

            self.kill_all(restart_groups=True)

            return Error.SUCCESS

    def _message_handle_routine(self):
        while True:
            try:
                while True:
                    message = self.__queue.get()
                    self.message_handler(message)
            except:
                self.logger.log_error(traceback.format_exc())

    def remove_task_group(self, group_id, ):
        self.__lock.acquire()
        try:
            if group_id in self.__groups:
                task_status = {
                    "finished": len(self.__groups[group_id].finished_tasks),
                    "aborted": len(self.__groups[group_id].aborted_tasks),
                    "total": self.__groups[group_id].task_num
                }
                self.__groups.pop(group_id)
            else:
                return Error.ERROR_TASK_GROUP_NOT_EXISTS
        finally:
            self.__lock.release()

        print("remove_task_group:task group '{}' finish".format(group_id))
        self.logger.log_info("task group '{}' finish".format(group_id))
        MessageSender.send_finish_ack(group_id, task_status, self.logger)

        return Error.SUCCESS

    def run(self):
        self._message_handle_routine()

    def kill_all(self, restart_groups=False):
        self.__lock.acquire()
        try:

            # terminate pool
            try:
                self.__pool.terminate()
                self.__pool.join()
                self.logger.log_info("pool tasks terminated")
            except Exception as e:
                self.logger.log_error(traceback.format_exc())

            try:
                # clean queue
                while not self.__queue.empty():
                    self.__queue.get_nowait()
                self.logger.log_info("queue message cleaned")
            except Exception as e:
                self.logger.log_error(traceback.format_exc())

            # reset task and group list
            self.__tasks = {}
            if not restart_groups:
                self.__groups = {}

            # restart pool
            self.__pool = multiprocessing.Pool(processes=self.__process_num)
            if restart_groups:
                for group_id in self.__groups:
                    group = self.__groups[group_id]
                    for task_id in group.running_tasks:
                        task = group.running_tasks[task_id]
                        self.apply_task(task, group_id)

        finally:
            self.__lock.release()

        return Error.SUCCESS


class TaskConst(object):
    """
        Some task relative consts
    """
    class TaskType(object):
        UPDATE_FACTOR_TASK = 1
        UPDATE_TICK_DATA_TASK = 2

    class TaskParam(object):
        TASK_MANAGER_QUEUE = "__TASK_MANAGER_QUEUE__"
        TASK_ID = "__TASK_ID__"
        TASK_GROUP_ID = "__TASK_GROUP_ID__"
        LOG_STACK = "__LOG_STACK__"


