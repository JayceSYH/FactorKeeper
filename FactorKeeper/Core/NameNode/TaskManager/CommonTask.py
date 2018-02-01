from Core.Error.Error import Error
import datetime, threading


class BaseTask(object):
    """
        This class defines a abstract task and interfaces.
        If you want to add a new task, make sure to implement
        "task_str" function to show task info properly and define
        "TASK_TYPE" field to identify task type.
    """
    STATUS_RUNNING = "Running"
    STATUS_WAITING_DEPENDENCY = "Waiting Dependencies"
    STATUS_READY = "Ready To Run"

    def __init__(self, task_desc, worker_info):
        self.__task_desc = task_desc
        self.__create_time = datetime.datetime.now()
        self.__worker_info = worker_info
        self.__is_sub_task = False
        self.__dependency_list = []
        self.__all_dependencies = []
        self.__notify_list = []
        self.__status = BaseTask.STATUS_READY
        self.__lock = threading.Lock()

    def finish_task_normally(self):
        self.__lock.acquire()
        try:
            for task in self.__notify_list:
                task.notify_task_finish(self)
            self.__notify_list = []
        finally:
            self.__lock.release()
        return Error.SUCCESS

    def notify_task_finish(self, task):
        self.__lock.acquire()
        try:
            if task in self.__dependency_list:
                self.__dependency_list.remove(task)
                if len(self.__dependency_list) == 0:
                    self.__status = self.STATUS_READY
        finally:
            self.__lock.release()

    def add_task_notified(self, task):
        self.__lock.acquire()
        try:
            self.__notify_list.append(task)
            self.__is_sub_task = True
        finally:
            self.__lock.release()
        return Error.SUCCESS

    def add_dependency(self, task):
        self.__lock.acquire()
        try:
            self.__dependency_list.append(task)
            self.__all_dependencies.append(task)
            self.__status = self.STATUS_WAITING_DEPENDENCY
        finally:
            self.__lock.release()
        task.add_task_notified(self)
        return Error.SUCCESS

    def get_runnable_dependencies(self):
        self.__lock.acquire()
        try:
            runnable_dependencies = [task for task in self.__dependency_list if task.status == self.STATUS_READY]
            for task in self.__dependency_list:
                err, deps = task.get_runnable_dependencies()
                if not err:
                    runnable_dependencies += deps
        finally:
            self.__lock.release()

        return Error.SUCCESS, runnable_dependencies

    def set_worker(self, worker_info):
        self.__lock.acquire()
        try:
            self.__worker_info = worker_info
            if worker_info is None:
                self.__status = self.STATUS_READY if self.__status in [self.STATUS_READY, self.STATUS_RUNNING] \
                    else self.STATUS_WAITING_DEPENDENCY
            else:
                self.__status = self.STATUS_RUNNING
        finally:
            self.__lock.release()

    def task_str(self, deps_status):
        pass

    def is_sub_task(self):
        return self.__is_sub_task

    @staticmethod
    def get_desc_from_id(task_id):
        pos = task_id.find("#")
        return task_id[pos + 1:]

    @property
    def task_id(self):
        return "{0}#{1}".format(self.__create_time, self.__task_desc)

    @property
    def task_desc(self):
        return self.__task_desc

    @property
    def status(self):
        return self.__status

    @property
    def all_dependencies(self):
        return self.__all_dependencies

    @property
    def status_desc(self):
        status = self.status
        if status == BaseTask.STATUS_RUNNING:
            return "Task is running"
        elif status == BaseTask.STATUS_READY:
            return "Task is ready to run"
        elif status == BaseTask.STATUS_WAITING_DEPENDENCY:
            return "Task is waiting for dependencies"

    @property
    def worker_info(self):
        return self.__worker_info

    @property
    def create_time(self):
        return self.__create_time

    def __str__(self):
        self.__lock.acquire()
        try:
            dependency_str_list = [str(task) for task in self.__dependency_list]
        finally:
            self.__lock.release()

        indent = "&nbsp" * 15
        dependency_str_list = [dependency_str.replace("<br>", "<br>" + indent) for dependency_str in dependency_str_list]
        dependency_status = "<br>".join(dependency_str_list)
        status = self.task_str(dependency_status)
        return status

    __repr__ = __str__


class FinishedTask(BaseTask):
    """
        Finished task is used to show finished tasks when querying finished task list
    """
    def __init__(self, task_id, task_type, commit_time, finish_time, final_status,
                 total_tasks=None, finished_num=None, aborted_num=None, worker_id=None):
        if total_tasks is not None and finished_num is not None and aborted_num is not None \
                and worker_id is not None:
            super().__init__(self.get_desc_from_id(task_id), None)
            self.task_type = task_type
            self.commit_time = commit_time
            self.finish_time = finish_time
            self.final_status = final_status
            self.total_task_num = total_tasks
            self.finished_task_num = finished_num
            self.aborted_task_num = aborted_num
            self.finish_task_id = task_id
            self.worker_id = worker_id
        else:
            assert 0, "Unimplemented Finished Task"

    def task_str(self, deps_status):
        task_str = "Task Id: {0} <br>" + \
                   "Task Type: {1} <br>" + \
                   "Commit Time: {2} <br>" +\
                   "Finish Time: {3} <br>" +\
                   "Final Status: {4} <br>" +\
                   "Total Tasks: {5} <br>" +\
                   "Finished Tasks: {6} <br>" +\
                   "Aborted Tasks: {7} <br>" + \
                   "Last Responsible Worker: {8} <br>" +\
                   "Dependencies: <br>" + \
                   "&nbsp" * 10 + "[{9} <br>" +\
                   "&nbsp" * 10 + "]"

        task_str = task_str.format(self.finish_task_id, self.task_type, self.commit_time, self.finish_time,
                                   self.final_status, self.total_task_num, self.finished_task_num,
                                   self.aborted_task_num, self.worker_id, deps_status)

        task_str = task_str.replace("<br>", "<br>|")
        task_str = "<br>" + "_" * 100 + "<br>|" + task_str + "<br>|" + "_" * 100

        return task_str
